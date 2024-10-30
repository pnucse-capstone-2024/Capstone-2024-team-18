use std::any;
use std::borrow::Borrow;
use std::cmp::{self};
use std::collections::BinaryHeap;
use std::f32::consts::E;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (i, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(i, iter));
            }
        }
        Self {
            current: heap.pop(),
            iters: heap,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let curr_iter = self.current.as_mut().unwrap().1.as_mut();
        let old_key = curr_iter.key().to_key_vec();

        match curr_iter.next() {
            // 1. curr_iter is not run out
            Ok(()) if curr_iter.is_valid() => {
                self.iters.push(self.current.take().unwrap());
                self.current = self.iters.pop();
            }
            // 2.curr_iter is run out and all iters are run out
            Ok(()) if !curr_iter.is_valid() && self.iters.is_empty() => {
                self.current = None;
                return Ok(());
            }
            // 3. curr_iter is run out and there are still some iters
            Ok(()) => {
                self.current = self.iters.pop();
            }
            Err(e) => return Err(e),
        }

        // check if the new key is the same as the old key
        if self.current.is_some() {
            let new_key = self.current.as_ref().unwrap().1.key().to_key_vec();
            if new_key == old_key {
                self.next()?;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|x| x.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map(|x| x.1.num_active_iterators())
                .unwrap_or(0)
    }
}
