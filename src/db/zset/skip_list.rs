use bytes::Bytes;
use rand::Rng;

const ZSKIPLIST_MAXLEVEL: usize = 32;
const ZSKIPLIST_P: f64 = 0.25;
pub const NULL_NODE: usize = usize::MAX;

#[derive(Clone, Debug)]
pub struct SkipListLevel {
    pub forward: usize,
    pub span: usize,
}

#[derive(Clone, Debug)]
pub struct Node {
    pub score: f64,
    pub member: Bytes,
    pub backward: usize,
    pub level: Vec<SkipListLevel>,
}

#[derive(Clone, Debug)]
pub struct SkipList {
    pub nodes: Vec<Node>,
    pub free_list: Vec<usize>,
    pub head: usize,
    pub tail: usize,
    pub max_level: usize,
    pub length: usize,
}

impl SkipList {
    pub fn new() -> Self {
        let mut sl = SkipList {
            nodes: Vec::new(),
            free_list: Vec::new(),
            head: 0,
            tail: NULL_NODE,
            max_level: 1,
            length: 0,
        };
        // 节点 0 作为 header，它的 score 和 member 都是空的（不关心）
        let header = Node {
            score: 0.0,
            member: Bytes::new(),
            backward: NULL_NODE,
            level: vec![
                SkipListLevel {
                    forward: NULL_NODE,
                    span: 0,
                };
                ZSKIPLIST_MAXLEVEL
            ],
        };
        sl.nodes.push(header);
        sl
    }

    fn random_level() -> usize {
        let mut level = 1;
        let mut rng = rand::thread_rng();
        while (rng.r#gen::<f64>() < ZSKIPLIST_P) && (level < ZSKIPLIST_MAXLEVEL) {
            level += 1;
        }
        level
    }

    fn allocate_node(&mut self, score: f64, member: Bytes, level_count: usize) -> usize {
        let node = Node {
            score,
            member,
            backward: NULL_NODE,
            level: vec![
                SkipListLevel {
                    forward: NULL_NODE,
                    span: 0,
                };
                level_count
            ],
        };
        if let Some(idx) = self.free_list.pop() {
            self.nodes[idx] = node;
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(node);
            idx
        }
    }

    pub fn insert(&mut self, score: f64, member: Bytes) -> usize {
        let mut update = [0; ZSKIPLIST_MAXLEVEL];
        let mut rank = [0; ZSKIPLIST_MAXLEVEL];
        let mut current = self.head;

        for i in (0..self.max_level).rev() {
            rank[i] = if i == self.max_level - 1 { 0 } else { rank[i + 1] };
            while self.nodes[current].level[i].forward != NULL_NODE {
                let next_idx = self.nodes[current].level[i].forward;
                let next_node = &self.nodes[next_idx];
                if next_node.score < score || (next_node.score == score && next_node.member < member) {
                    rank[i] += self.nodes[current].level[i].span;
                    current = next_idx;
                } else {
                    break;
                }
            }
            update[i] = current;
        }

        let level = Self::random_level();
        if level > self.max_level {
            for i in self.max_level..level {
                rank[i] = 0;
                update[i] = self.head;
                self.nodes[self.head].level[i].span = self.length;
            }
            self.max_level = level;
        }

        let new_idx = self.allocate_node(score, member, level);

        for i in 0..level {
            let up_idx = update[i];
            self.nodes[new_idx].level[i].forward = self.nodes[up_idx].level[i].forward;
            self.nodes[up_idx].level[i].forward = new_idx;

            let span_diff = rank[0] - rank[i];
            self.nodes[new_idx].level[i].span = self.nodes[up_idx].level[i].span - span_diff;
            self.nodes[up_idx].level[i].span = span_diff + 1;
        }

        for i in level..self.max_level {
            self.nodes[update[i]].level[i].span += 1;
        }

        let prev_idx = update[0];
        self.nodes[new_idx].backward = if prev_idx == self.head { NULL_NODE } else { prev_idx };

        if self.nodes[new_idx].level[0].forward != NULL_NODE {
            let next_idx = self.nodes[new_idx].level[0].forward;
            self.nodes[next_idx].backward = new_idx;
        } else {
            self.tail = new_idx;
        }

        self.length += 1;
        new_idx
    }

    pub fn delete(&mut self, score: f64, member: &Bytes) -> bool {
        let mut update = [0; ZSKIPLIST_MAXLEVEL];
        let mut current = self.head;

        for i in (0..self.max_level).rev() {
            while self.nodes[current].level[i].forward != NULL_NODE {
                let next_idx = self.nodes[current].level[i].forward;
                let next_node = &self.nodes[next_idx];
                if next_node.score < score || (next_node.score == score && next_node.member < *member) {
                    current = next_idx;
                } else {
                    break;
                }
            }
            update[i] = current;
        }

        let target_idx = self.nodes[current].level[0].forward;
        if target_idx != NULL_NODE {
            let target_node = &self.nodes[target_idx];
            if target_node.score == score && target_node.member == *member {
                self.delete_node(target_idx, &update);
                return true;
            }
        }
        false
    }

    fn delete_node(&mut self, target_idx: usize, update: &[usize; ZSKIPLIST_MAXLEVEL]) {
        let level_count = self.nodes[target_idx].level.len();
        for i in 0..self.max_level {
            if self.nodes[update[i]].level[i].forward == target_idx {
                self.nodes[update[i]].level[i].span += self.nodes[target_idx].level[i].span - 1;
                self.nodes[update[i]].level[i].forward = self.nodes[target_idx].level[i].forward;
            } else {
                self.nodes[update[i]].level[i].span -= 1;
            }
        }

        if self.nodes[target_idx].level[0].forward != NULL_NODE {
            let next_idx = self.nodes[target_idx].level[0].forward;
            self.nodes[next_idx].backward = self.nodes[target_idx].backward;
        } else {
            self.tail = self.nodes[target_idx].backward;
        }

        while self.max_level > 1 && self.nodes[self.head].level[self.max_level - 1].forward == NULL_NODE {
            self.max_level -= 1;
        }

        self.length -= 1;
        self.nodes[target_idx].score = 0.0;
        self.nodes[target_idx].member = Bytes::new();
        self.free_list.push(target_idx);
    }

    pub fn get_rank(&self, score: f64, member: &Bytes) -> Option<usize> {
        let mut rank = 0;
        let mut current = self.head;
        for i in (0..self.max_level).rev() {
            while self.nodes[current].level[i].forward != NULL_NODE {
                let next_idx = self.nodes[current].level[i].forward;
                let next_node = &self.nodes[next_idx];
                if next_node.score < score || (next_node.score == score && next_node.member <= *member) {
                    rank += self.nodes[current].level[i].span;
                    current = next_idx;
                } else {
                    break;
                }
            }
        }
        if current != self.head && self.nodes[current].member == *member {
            Some(rank - 1) // 0-indexed
        } else {
            None
        }
    }
    
    pub fn get_element_by_rank(&self, rank: usize) -> Option<(f64, Bytes)> {
        if rank >= self.length {
            return None;
        }
        // 1-based rank internal calculation
        let target = rank + 1;
        let mut traversed = 0;
        let mut current = self.head;

        for i in (0..self.max_level).rev() {
            while self.nodes[current].level[i].forward != NULL_NODE {
                let span = self.nodes[current].level[i].span;
                if traversed + span <= target {
                    traversed += span;
                    current = self.nodes[current].level[i].forward;
                } else {
                    break;
                }
            }
            if traversed == target {
                let node = &self.nodes[current];
                return Some((node.score, node.member.clone()));
            }
        }
        None
    }
}
