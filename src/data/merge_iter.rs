use std::iter::Peekable;

struct MergeIter<I1, I2>
where
    I1: Iterator,
    I2: Iterator<Item = I1::Item>,
    I1::Item: Ord,
{
    iter1: Peekable<I1>,
    iter2: Peekable<I2>,
}

impl<I1, I2> Iterator for MergeIter<I1, I2>
where
    I1: Iterator,
    I2: Iterator<Item = I1::Item>,
    I1::Item: Ord,
{
    type Item = I1::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.iter1.peek(), self.iter2.peek()) {
            (Some(&v1), Some(&v2)) if v1 <= v2 => self.iter1.next(),
            (Some(_), Some(_)) => self.iter2.next(),
            (Some(_), None) => self.iter1.next(),
            (None, Some(_)) => self.iter2.next(),
            (None, None) => None,
        }
    }
}

fn merge_sorted<I1, I2>(iter1: I1, iter2: I2) -> MergeIter<I1::IntoIter, I2::IntoIter>
where
    I1: IntoIterator,
    I2: IntoIterator<Item = I1::Item>,
    I1::Item: Ord,
{
    MergeIter {
        iter1: iter1.into_iter().peekable(),
        iter2: iter2.into_iter().peekable(),
    }
}