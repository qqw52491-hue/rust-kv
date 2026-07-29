#[cfg(test)]
mod tests {
    use crate::config::EvictionType;
    use crate::context::{CONN_STATE, ConnectionContent, ConnectionState};
    use crate::db::Db;
    use crate::error::{BLPopCommand, Command, Frame, KvError, LPopCommand, LPushCommand};
    use crate::executor::{CommandContext, Executor};
    use crate::lua::lua_work::{LuaRouter, LuaTask};
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::time::Duration;

    fn create_dummy_ctx(db: Db) -> CommandContext {
        let (aof_tx, _) = mpsc::channel(100);
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        let (lua_sender_tx, _) = mpsc::channel(1);
        let (_, receivce_lua) = flume::bounded(1);

        let lua_sender = LuaRouter {
            senders: vec![lua_sender_tx],
        };

        CommandContext::Normal {
            db,
            connect_content: ConnectionContent {
                aof_tx,
                shutdown_tx,
                lua_sender,
                receivce_lua,
            },
        }
    }

    #[tokio::test]
    async fn test_blpop_timeout() {
        let db = Db::new(&EvictionType::LRU);
        let ctx = create_dummy_ctx(db.clone());
        let key = Arc::new("test_list".to_string());

        let blpop = BLPopCommand {
            key: key.clone(),
            timeout: 1, // 1 second timeout
        };

        // Run in CONN_STATE scope
        let state = ConnectionState {
            selected_db: 0,
            client_address: None,
        };
        let res = CONN_STATE
            .scope(state, async {
                let start = tokio::time::Instant::now();
                let frame = blpop.execute(ctx).await.unwrap();
                let elapsed = start.elapsed();
                (frame, elapsed)
            })
            .await;

        assert_eq!(res.0, Frame::Null); // Timeout returns Null
        assert!(res.1 >= Duration::from_secs(1)); // Should have waited at least 1 second
    }

    #[tokio::test]
    async fn test_blpop_wakeup() {
        let db = Db::new(&EvictionType::LRU);
        let ctx1 = create_dummy_ctx(db.clone());
        let ctx2 = create_dummy_ctx(db.clone());
        let key = Arc::new("test_list2".to_string());

        let blpop = BLPopCommand {
            key: key.clone(),
            timeout: 5, // 5 second timeout, but we will wake it up
        };

        let lpush = LPushCommand {
            key: key.clone(),
            values: vec![bytes::Bytes::from_static(b"hello")],
        };

        // Run BLPOP in background task
        let state = ConnectionState {
            selected_db: 0,
            client_address: None,
        };
        let state2 = state.clone();

        let handle = tokio::spawn(async move {
            CONN_STATE
                .scope(state, async move { blpop.execute(ctx1).await.unwrap() })
                .await
        });

        // Give BLPOP time to register and block
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Run LPUSH to wake it up
        CONN_STATE
            .scope(state2, async move {
                let push_res = lpush.execute(ctx2).await.unwrap();
                // push_res will be 0 since the value went straight into the channel!
                assert_eq!(push_res, Frame::Integer(0));
            })
            .await;

        let blpop_res = handle.await.unwrap();
        // The blpop result should be Array [key, value]
        match blpop_res {
            Frame::Array(frames) => {
                assert_eq!(frames.len(), 2);
                if let Frame::Bulk(b) = &frames[1] {
                    assert_eq!(b.as_ref(), b"hello");
                } else {
                    panic!("Expected Bulk frame for value");
                }
            }
            _ => panic!("Expected Array frame"),
        }
    }
}
