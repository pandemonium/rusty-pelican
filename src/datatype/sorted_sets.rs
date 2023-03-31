use std::io;
use std::cmp;
use std::time;
use std::collections;
use serde::*;

use crate::commands;
use crate::core;
use crate::resp;

#[derive(Clone, Debug, PartialEq)]
pub enum SortedSetApi {
    Add { key: String, entries: Vec<(f64, String)> },
    RangeByRank(String, usize, usize),
    RangeByScore(String, f64, f64),
    Rank(String),
    Score(String),
}

pub struct MemberEntry {
    rank: usize,
    score: f64,
    member: String,
}

impl MemberEntry {
    fn new(rank: usize, score: f64, member: &str) -> Self {
        Self { rank, score, member: member.into() }
    }
}

pub enum AddOption {
    UpdateOnly,             /* XX */
    AddOnly,                /* NX */
    AddOrUpdateLessThan,    /* LT */
    AddOrUpdateGreaterThan, /* GT */
}

pub trait SortedSet {
    fn add(&mut self, key: &str, entries: &[(f64, &str)], merge: AddOption) -> usize;
    fn range_by_rank(&self, key: &str, start: usize, stop: usize) -> Vec<MemberEntry>;
    fn range_by_score(&self, key: &str, start: f64, stop: f64) -> Vec<MemberEntry>;
    fn member_stats(&self, key: &str, member: &str) -> Option<MemberEntry>;
}

impl SortedSet for core::Domain {
    fn add(&mut self, key: &str, entries: &[(f64, &str)], merge: AddOption) -> usize {
        let mut added_count = 0;
        self.sorted_sets
            .entry(key.into()).and_modify(|xs| {
                entries.iter().cloned().for_each(|(score, member)| {
                    xs.merge(score, member);
                    added_count += 1;
                });
             })
            .or_insert_with(|| {
                let mut xs = OrderedScores::new();
                entries.iter().cloned().for_each(|(score, member)| {
                    xs.merge(score, member);
                    added_count += 1;
                });
                xs
             });
        added_count
    }

    fn range_by_rank(&self, key: &str, start: usize, stop: usize) -> Vec<MemberEntry> {
        self.sorted_sets.get(key.into())
            .map_or(vec![], |xs|
                xs.range_by_rank(start, stop).map(|(rank, (score, member))| {
                    MemberEntry::new(rank, score, &member)
                }).collect()
            )
    }

    fn range_by_score(&self, key: &str, start: f64, stop: f64) -> Vec<MemberEntry> {
        todo!()
    }

    fn member_stats(&self, key: &str, member: &str) -> Option<MemberEntry> {
        todo!()
    }
}

pub fn apply(
    state:   &core::DomainContext,
    command: core::CommandContext<SortedSetApi>
) -> Result<resp::Message, io::Error> {
//    match &*command {
//        SortedSetApi::Add { key: (), entries: () } => {}
//        SortedSetApi::RangeByRank(_, _, _) => todo!(),
//        SortedSetApi::RangeByScore(_, _, _) => todo!(),
//        SortedSetApi::Rank(_) => todo!(),
//        SortedSetApi::Score(_) => todo!(),
//    }
    todo!()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Score(f64);
impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.0.total_cmp(&other.0) }
}
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}
impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool { self.0 == other.0 }
}
impl Eq for Score {}

#[derive(Deserialize, Serialize)]
pub struct OrderedScores {
    member_to_score:  collections::HashMap<String, Score>,
    score_to_members: collections::BTreeMap<Score, collections::BTreeSet<String>>,
}

impl OrderedScores {
    fn new() -> Self {
        Self {
            member_to_score: collections::HashMap::new(),
            score_to_members: collections::BTreeMap::new(),
        }
    }

    /* This should probably have the rank included. */
    fn range_by_score(&self, start: f64, stop: f64) -> impl Iterator<Item = (usize, (f64, String))> + '_ {
        /* Make sure start < stop. (Otherwise, a reverse iteration is selected.) */
        self.score_to_members
            .range(Score(start) ..= Score(stop))
            .flat_map(|(Score(score), members)|
                members.iter().map(|member| (*score, member.clone()))
             )
            .enumerate()
    }

    fn range_by_rank(&self, start: usize, stop: usize) -> impl Iterator<Item = (usize, (f64, String))> + '_ {
        self.score_to_members
            .iter()
            .flat_map(|(Score(score), members)|
                members.iter().map(|member| (*score, member.clone()))
             )
            .enumerate()
            .skip(start).take(stop - start)
    }

    fn member_stats(&self, member: &str) -> Option<MemberEntry> {
        let Score(score) = self.member_to_score.get(member)?;
        self.range_by_score(f64::MIN, *score)
            .find_map(|(rank, (score, subject))| 
                (member == subject).then(|| MemberEntry::new(rank, score, member))
            )
    }

    /* Add parameter to control how or if a new score is incorporated. */
    fn merge(&mut self, new_score: f64, member: &str) {
        match self.member_to_score.entry(member.into()) {
            collections::hash_map::Entry::Occupied(mut member_entry) => {
                let current_score = member_entry.get().clone();
                /* This begs for a re-think about the if-statement. */
                if let collections::btree_map::Entry::Occupied(mut score_entry) = self.score_to_members.entry(current_score) {
                    let members = score_entry.get_mut();
                    if members.remove(member) && members.is_empty() {
                        score_entry.remove_entry();
                    }
                    member_entry.insert(Score(new_score));
                    self.score_to_members.entry(Score(new_score))
                        .and_modify(|e| { e.insert(member.into()); })
                        .or_insert_with(|| { collections::BTreeSet::from([ member.into() ]) });    
                } else {
                    panic!("member_to_score <=> score_to_member invariant broken")
                }                        
            }
            collections::hash_map::Entry::Vacant(e) => {
                e.insert(Score(new_score));
                self.score_to_members.entry(Score(new_score))
                    .and_modify(|_| { panic!("score_to_member <=> member_to_score invariant broken") })
                    .or_insert_with(|| { collections::BTreeSet::from([ member.into() ]) });    
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn or_this() {
        let mut d = OrderedScores::new();

        d.merge(1f64, "user:1");
        assert_eq!(d.member_to_score.get("user:1").unwrap(), &Score(1f64));
        assert_eq!(
            d.score_to_members.get(&Score(1f64)).unwrap(), &collections::BTreeSet::from(["user:1".to_string()])
        );
        assert_eq!(d.member_to_score.len(), 1);
        assert_eq!(d.score_to_members.len(), 1);

        d.merge(2f64, "user:1");
        assert_eq!(d.member_to_score.get("user:1").unwrap(), &Score(2f64));
        assert_eq!(
            d.score_to_members.get(&Score(2f64)).unwrap(), 
            &collections::BTreeSet::from(["user:1".to_string()])
        );
        assert_eq!(d.member_to_score.len(), 1);
        assert_eq!(d.score_to_members.len(), 1);

        d.merge(1f64, "user:2");
        assert_eq!(d.member_to_score.get("user:2").unwrap(), &Score(1f64));
        assert_eq!(d.score_to_members.get(
            &Score(1f64)).unwrap(), &collections::BTreeSet::from(["user:2".to_string()])
        );
        assert_eq!(d.member_to_score.len(), 2);
        assert_eq!(d.score_to_members.len(), 2);

        assert_eq!(
            d.range_by_score(0f64, 100f64).collect::<Vec<_>>(), 
            vec![ (0, (1f64, "user:2".to_string())), (1, (2f64, "user:1".to_string())) ]
        );

        assert_eq!(
            d.range_by_rank(0, 100).collect::<Vec<_>>(), 
            vec![ (0, (1f64, "user:2".to_string())), (1, (2f64, "user:1".to_string())) ]
        );

        d.merge(2f64, "user:2");
        assert_eq!(d.member_to_score.get("user:2").unwrap(), &Score(2f64));
        assert_eq!(
            d.score_to_members.get(&Score(2f64)).unwrap(),
            &collections::BTreeSet::from([ "user:2".to_string(), "user:1".to_string() ]));
        assert_eq!(d.member_to_score.len(), 2);
        assert_eq!(d.score_to_members.len(), 1);

        assert_eq!(
            d.range_by_score(0f64, 100f64).collect::<Vec<_>>(), 
            vec![ (0, (2f64, "user:1".to_string())), (1, (2f64, "user:2".to_string())) ]
        );

        assert_eq!(
            d.range_by_rank(0, 100).collect::<Vec<_>>(), 
            vec![ (0, (2f64, "user:1".to_string())), (1, (2f64, "user:2".to_string())) ]
        );

        d.merge(3f64, "user:3");
        assert_eq!(d.member_to_score.get("user:3").unwrap(), &Score(3f64));
        assert_eq!(
            d.score_to_members.get(&Score(3f64)).unwrap(), 
            &collections::BTreeSet::from([ "user:3".to_string() ]));
        assert_eq!(d.member_to_score.len(), 3);
        assert_eq!(d.score_to_members.len(), 2);

        assert_eq!(
            d.range_by_score(0f64, 100f64).collect::<Vec<_>>(), 
            vec![ 
                (0, (2f64, "user:1".to_string())),
                (1, (2f64, "user:2".to_string())),
                (2, (3f64, "user:3".to_string())),
            ]
        );

        assert_eq!(
            d.range_by_rank(0, 100).collect::<Vec<_>>(), 
            vec![ 
                (0, (2f64, "user:1".to_string())),
                (1, (2f64, "user:2".to_string())),
                (2, (3f64, "user:3".to_string())),
            ]
        );

        assert_eq!(
            d.range_by_rank(1, 100).collect::<Vec<_>>(), 
            vec![
                (1, (2f64, "user:2".to_string())),
                (2, (3f64, "user:3".to_string())),
            ]
        );

        assert_eq!(d.member_stats("user:1").unwrap().rank, 0);
        assert_eq!(d.member_stats("user:2").unwrap().rank, 1);
        assert_eq!(d.member_stats("user:3").unwrap().rank, 2);
    }

}