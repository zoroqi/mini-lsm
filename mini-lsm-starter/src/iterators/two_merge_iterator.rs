#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::StorageIterator;
use anyhow::Result;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    choose_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = TwoMergeIterator { a, b, choose_a: true };
        iter.choose()?;
        Ok(iter)
    }
    fn choose(&mut self) -> Result<()> {
        match (self.a.is_valid(), self.b.is_valid()) {
            (false, true) => {
                self.choose_a = false;
            }
            (true, false) => {
                self.choose_a = true;
            }
            (true, true) => {
                let cmp = self.a.key().cmp(&self.b.key());
                match cmp {
                    std::cmp::Ordering::Less => {
                        self.choose_a = true;
                    }
                    std::cmp::Ordering::Greater => {
                        self.choose_a = false;
                    }
                    std::cmp::Ordering::Equal => {
                        self.b.next()?;
                        self.choose_a = true;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    // 如何实现这么一个函数? current_iter()时, 返回当前的迭代器, 方便在执行 key, value, is_valid 代码更简单一些.
    // fn current_iter(&self) -> impl StorageIterator {
    //     if self.a_or_b {
    //         &self.a
    //     } else {
    //         &self.b
    //     }
    // }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.choose_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.choose_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.choose_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.choose_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.choose()
    }
}
