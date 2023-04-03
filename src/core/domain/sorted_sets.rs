use std::io;
use std::cmp;
use std::collections;
use serde::*;

use crate::core;
use crate::core::resp;

#[derive(Clone, Debug, PartialEq)]
pub enum SortedSetApi {
    Add { key: String, entries: Vec<(f64, String)>, options: AddOptions, },
    RangeByRank(String, usize, usize),
    RangeByScore(String, f64, f64),
    Rank(String, String),
    Score(String, String),
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

#[derive(Clone, Debug, PartialEq)]
pub enum Only {
    UpdateExisting,             /* XX */
    AddNew,                     /* NX */
}

#[derive(Clone, Debug, PartialEq)]
pub enum When {
    LessThan,                   /* LT */
    GreaterThan,                /* GT */
}

#[derive(Clone, Debug, PartialEq)]
pub enum MergePolicy {
    Require(Only),              /* XX | NX */
    UpdateExisting(When),       /* XX + (GT | LT) */
    AddOrUpdate(When),          /* GT | LT */
    Default,                    /* No options specified. */
    Diverged(String),           /* Bad combination. */
}


#[derive(Clone, Debug, PartialEq)]
pub enum Return {
    Added,                      /* Nothing */
    Changed,                    /* CH */
}

impl Return {
    fn default() -> Self { Return::Added }

    fn parse(word: &str) -> Option<Return> {
        if matches!(word, "CH" | "ch") {
            Some(Return::Changed)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AddOptions {
    merge: MergePolicy,
    and_return: Return,
}

impl Default for AddOptions {
    fn default() -> Self {
        Self::return_default(MergePolicy::Default)
    }
}

impl AddOptions {
    fn select_return(p: &AddOptions, q: &AddOptions, merge: MergePolicy) -> Self {
        if p.and_return == Return::Changed || q.and_return == Return::Changed {
            Self { merge, and_return: Return::Changed }
        } else {
            Self { merge, and_return: Return::default() }
        }
    }

    fn merge_policy(&self) -> &MergePolicy {
        &self.merge
    }

    fn return_default(merge: MergePolicy) -> Self {
        Self { merge, and_return: Return::default() }
    }

    fn return_changed(merge: MergePolicy) -> Self {
        Self { merge, and_return: Return::Changed }
    }

    fn parse(word: &str) -> Option<Self> {
        When::parse(word).map(|when|
            Self::return_default(MergePolicy::AddOrUpdate(when))
        ).or_else(||
            Only::parse(word).map(|only| Self::return_default(MergePolicy::Require(only)))
        ).or_else(||
            Return::parse(word).map(|_| Self::return_changed(MergePolicy::Default))
        )
    }

    fn combine(lhs: Self, rhs: &Self) -> Self {
        match (lhs.merge_policy(), rhs.merge_policy()) {
            (MergePolicy::AddOrUpdate(when), MergePolicy::Require(Only::UpdateExisting)) |
            (MergePolicy::Require(Only::UpdateExisting), MergePolicy::AddOrUpdate(when)) =>
                Self::select_return(&lhs, rhs, MergePolicy::UpdateExisting(when.clone())),
            otherwise =>
                Self::return_default(MergePolicy::Diverged(format!("bad options: {otherwise:?}"))),
        }
    }
}

pub struct AddArgsParser {
    pub options: AddOptions,
    pub entries: Vec<String>,
}

impl AddArgsParser {
    pub fn new() -> Self {
        Self { options: AddOptions::default(), entries: Vec::new() }
    }

    pub fn parse_into(&mut self, phrase: &[&str]) {
        let mut options = vec![];
        for word in phrase {
            if !self.entries.is_empty() {
                self.entries.push((*word).to_string());
            } else if let Some(option) = AddOptions::parse(word) {
                options.push(option);
            } else {
                self.entries.push((*word).to_string());
            }
        }
        self.options = options.iter().fold(AddOptions::default(), AddOptions::combine);
    }
}

impl Only {
    fn parse(word: &str) -> Option<Only> {
        match word {
            "XX" | "xx" => Some(Only::UpdateExisting),
            "NX" | "nx" => Some(Only::AddNew),
            _otherwise  => None,
        }
    }
}

impl When {
    fn parse(word: &str) -> Option<When> {
        match word {
            "GT" | "gt" => Some(When::GreaterThan),
            "LT" | "lt" => Some(When::LessThan),
            _otherwise  => None,
        }
    }
}

pub trait SortedSet {
    fn add(&mut self, key: &str, entries: &[(f64, &str)], options: AddOptions) -> usize;
    fn range_by_rank(&self, key: &str, start: usize, stop: usize) -> Vec<MemberEntry>;
    fn range_by_score(&self, key: &str, start: f64, stop: f64) -> Vec<MemberEntry>;
    fn member_stats(&self, key: &str, member: &str) -> Option<MemberEntry>;
}

impl SortedSet for core::Domain {
    fn add(&mut self, key: &str, entries: &[(f64, &str)], _options: AddOptions) -> usize {
        let mut count = 0;
        self.sorted_sets
            .entry(key.into()).and_modify(|xs|
                entries.iter().copied().for_each(|(score, member)| {
                    xs.merge(score, member);
                    count += 1;
                })
             )
            .or_insert_with(|| {
                let mut xs = OrderedScores::new();
                entries.iter().copied().for_each(|(score, member)| {
                    xs.merge(score, member);
                    count += 1;
                });
                xs
             });
        count
    }

    fn range_by_rank(&self, key: &str, start: usize, stop: usize) -> Vec<MemberEntry> {
        self.sorted_sets.get(key)
            .map_or(vec![], |xs|
                xs.range_by_rank(start, stop).map(|(rank, (score, member))| {
                    MemberEntry::new(rank, score, &member)
                }).collect()
            )
    }

    fn range_by_score(&self, key: &str, start: f64, stop: f64) -> Vec<MemberEntry> {
        self.sorted_sets.get(key)
            .map_or(vec![], |xs|
                xs.range_by_score(start, stop).map(|(rank, (score, member))| {
                    MemberEntry::new(rank, score, &member)
                }).collect()
            )
    }

    fn member_stats(&self, key: &str, member: &str) -> Option<MemberEntry> {
        self.sorted_sets.get(key)?.member_stats(member)
    }
}

pub fn apply(
    state:   &core::DomainContext,
    command: core::CommandContext<SortedSetApi>
) -> Result<resp::Message, io::Error> {
    match &*command {
        SortedSetApi::Add { key, entries, options } =>
            state.apply_transaction(&command, |data| {
                /* Why is this necessary? */
                let xs = entries.iter().map(|(a, b)| (*a, b.as_str())).collect::<Vec<(f64, &str)>>();
                resp::Message::Integer(
                    data.add(key, &xs, options.clone()) as i64
                )
            }),
        SortedSetApi::RangeByRank(key, start, stop) =>
            Ok(resp::Message::make_bulk_array(
                state.for_reading()?.range_by_rank(key, *start, *stop)
                     .iter().map(|x| x.member.clone()).collect::<Vec<_>>()
                     .as_slice()
            )),
        SortedSetApi::RangeByScore(key, start, stop) => 
            Ok(resp::Message::make_bulk_array(
                state.for_reading()?.range_by_score(key, *start, *stop)
                    .iter().map(|x| x.member.clone()).collect::<Vec<_>>()
                    .as_slice()
            )),
        SortedSetApi::Rank(key, member) =>
            if let Some(stat) = state.for_reading()?.member_stats(key, member) {
                Ok(resp::Message::Integer(stat.rank as i64))
            } else {
                Ok(resp::Message::Nil)
            },
        SortedSetApi::Score(key, member) =>
            if let Some(stat) = state.for_reading()?.member_stats(key, member) {
                Ok(resp::Message::BulkString(stat.score.to_string()))
            } else {
                Ok(resp::Message::Nil)
            },
}
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Score(f64);
impl Ord for Score {
    fn cmp(&self, other: &Self) -> cmp::Ordering { self.0.total_cmp(&other.0) }
}
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
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

use collections::hash_map::Entry as HashEntry;
use collections::btree_map::Entry as BTreeEntry;

impl OrderedScores {
    fn new() -> Self {
        Self {
            member_to_score: collections::HashMap::new(),
            score_to_members: collections::BTreeMap::new(),
        }
    }

    /* This should probably have the rank included. */
    fn range_by_score(&self, start: f64, stop: f64) -> impl Iterator<Item = (usize, (f64, String))> + '_ {
        /* Make sure start < stop. */
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
            HashEntry::Occupied(mut member_score) => {
                let score = member_score.get().clone();
                if let BTreeEntry::Occupied(mut score_entry) = self.score_to_members.entry(score) {
                    let members = score_entry.get_mut();
                    if members.remove(member) && members.is_empty() {
                        score_entry.remove_entry();
                    }
                    member_score.insert(Score(new_score));
                    self.score_to_members.entry(Score(new_score))
                        .and_modify(|e| { e.insert(member.into()); })
                        .or_insert_with(|| { collections::BTreeSet::from([ member.into() ]) });    
                } else {
                    panic!("member_to_score <=> score_to_member invariant broken; 
                            member has the score, but scores does not have that member")
                }
            }
            collections::hash_map::Entry::Vacant(e) => {
                e.insert(Score(new_score));
                self.score_to_members.entry(Score(new_score))
                    .and_modify(|e| { e.insert(member.into()); })
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