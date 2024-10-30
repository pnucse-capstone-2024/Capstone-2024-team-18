#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        return Ok(Self { a, b });
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;


    fn key(&self) -> Self::KeyType<'_> {
        if self.a.is_valid() && !self.b.is_valid(){
            return self.a.key();
        } else if !self.a.is_valid() && self.b.is_valid(){
            return self.b.key();
        }else if self.a.key() < self.b.key(){
            return self.a.key();
        }else if self.a.key() > self.b.key(){
            return self.b.key();
        }else {
            return self.a.key();
        }
    }

    fn value(&self) -> &[u8] {
        if self.a.is_valid() && !self.b.is_valid(){
            return self.a.value();
        } else if !self.a.is_valid() && self.b.is_valid(){
            return self.b.value();
        }else if self.a.key() < self.b.key(){
            return self.a.value();
        }else if self.a.key() > self.b.key(){
            return self.b.value();
        }else {
            return self.a.value();
        }
    }

    fn is_valid(&self) -> bool {
        return self.a.is_valid() || self.b.is_valid();
    }

    fn next(&mut self) -> Result<()> {
        assert!(self.is_valid());
        if self.a.is_valid() && !self.b.is_valid(){
            self.a.next()?;
        } else if !self.a.is_valid() && self.b.is_valid(){
            self.b.next()?;
        }else if self.a.key() < self.b.key(){
            self.a.next()?;
        }else if self.a.key() > self.b.key(){
            self.b.next()?;
        }else {
            self.a.next()?;
            self.b.next()?;
        }
        return Ok(());
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
