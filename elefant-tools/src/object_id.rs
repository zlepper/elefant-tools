#[derive(Copy, Clone, Debug, Default, PartialOrd)]
pub struct ObjectId {
    value: Option<usize>,
}

impl ObjectId {
    pub(crate) fn new(value: usize) -> Self {
        ObjectId { value: Some(value) }
    }
}

impl From<usize> for ObjectId {
    fn from(value: usize) -> Self {
        ObjectId::new(value)
    }
}

impl PartialEq for ObjectId {
    fn eq(&self, other: &Self) -> bool {
        if let (Some(self_value), Some(other_value)) = (self.value, other.value) {
            self_value == other_value
        } else {
            true
        }
    }
}

impl Eq for ObjectId {

}

pub struct ObjectIdGenerator {
    next_id: usize,
}

impl ObjectIdGenerator {
    pub fn new() -> Self {
        Self { next_id: 1 }
    }

    pub fn next(&mut self) -> ObjectId {
        let id = self.next_id;
        self.next_id += 1;
        ObjectId::new(id)
    }
}


pub trait HaveDependencies {
    fn depends_on(&self) -> &Vec<ObjectId>;
    fn object_id(&self) -> ObjectId;
}

pub trait DependencySortable: Iterator {
    fn sort_by_dependencies(self) -> Vec<Self::Item>;
}

impl<I> DependencySortable for I
    where I: Iterator + Sized,
          I::Item: HaveDependencies
{
    fn sort_by_dependencies(self) -> Vec<Self::Item> {



        let mut sorted: Vec<Self::Item> = self.collect();

        if sorted.is_empty() {
            return sorted;
        }


        // Move everything with 0 dependencies to the front

        let mut i = 0;
        let mut j = sorted.len() - 1;
        while i < j {
            if sorted[i].depends_on().is_empty() {
                i += 1;
            } else if !sorted[j].depends_on().is_empty() {
                j -= 1;
            } else {
                sorted.swap(i, j);
                i += 1;
                j -= 1;
            }
        }

        // Sort the rest by dependencies
        let mut swaps = 0;
        let max_swaps = sorted.len() * sorted.len();
        loop {
            let mut swapped = false;
            for i in 0..sorted.len() {
                for j in i + 1..sorted.len() {
                    if sorted[i].depends_on().contains(&sorted[j].object_id()) {
                        sorted.swap(i, j);
                        swapped = true;
                        swaps += 1;
                    }
                }
            }
            if !swapped {
                break;
            }

            if swaps > max_swaps {
                panic!("Circular dependencies detected");
            }
        }


        sorted
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::{Debug, Formatter};
    use std::panic::catch_unwind;
    use itertools::Itertools;
    use super::*;

    #[derive(Eq, Clone)]
    struct TestItem {
        object_id: ObjectId,
        depends_on: Vec<ObjectId>,
    }
    impl HaveDependencies for TestItem {
        fn depends_on(&self) -> &Vec<ObjectId> {
            &self.depends_on
        }

        fn object_id(&self) -> ObjectId {
            self.object_id
        }
    }

    impl PartialEq for TestItem {
        fn eq(&self, other: &Self) -> bool {
            self.object_id == other.object_id
        }
    }

    impl Debug for TestItem {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.object_id.value.unwrap())
        }
    }

    #[test]
    fn sorted_by_dependencies_1() {

        let items = vec![
            TestItem {
                object_id: 1.into(),
                depends_on: vec![],
            },
            TestItem {
                object_id: 2.into(),
                depends_on: vec![1.into()],
            },
            TestItem {
                object_id: 3.into(),
                depends_on: vec![1.into()],
            },
        ];

        for items in items.into_iter().permutations(3) {
            let sorted = items.into_iter().sort_by_dependencies();

            assert!(matches!(sorted[0].object_id.value.unwrap(), 1));
            assert!(matches!(sorted[1].object_id.value.unwrap(), 2|3));
            assert!(matches!(sorted[2].object_id.value.unwrap(), 2|3));
        }

    }

    #[test]
    fn sorted_by_dependencies_2() {

        let items = vec![
            TestItem {
                object_id: 1.into(),
                depends_on: vec![3.into()],
            },
            TestItem {
                object_id: 2.into(),
                depends_on: vec![],
            },
            TestItem {
                object_id: 3.into(),
                depends_on: vec![],
            },
        ];

        for items in items.into_iter().permutations(3) {
            let sorted = items.into_iter().sort_by_dependencies();

            assert!(matches!(sorted[0].object_id.value.unwrap(), 2|3));
            assert!(matches!(sorted[1].object_id.value.unwrap(), 2|3));
            assert!(matches!(sorted[2].object_id.value.unwrap(), 1));
        }
    }

    #[test]
    fn sorted_by_dependencies_3() {

        let items = vec![
            TestItem {
                object_id: 1.into(),
                depends_on: vec![3.into()],
            },
            TestItem {
                object_id: 2.into(),
                depends_on: vec![],
            },
            TestItem {
                object_id: 3.into(),
                depends_on: vec![2.into()],
            },
        ];

        for items in items.into_iter().permutations(3) {
            let sorted = items.into_iter().sort_by_dependencies();

            assert!(matches!(sorted[0].object_id.value.unwrap(), 2));
            assert!(matches!(sorted[1].object_id.value.unwrap(), 3));
            assert!(matches!(sorted[2].object_id.value.unwrap(), 1));
        }
    }

    #[test]
    fn sorted_by_dependencies_4() {

        let items = vec![
            TestItem {
                object_id: 1.into(),
                depends_on: vec![3.into(), 2.into()],
            },
            TestItem {
                object_id: 2.into(),
                depends_on: vec![],
            },
            TestItem {
                object_id: 3.into(),
                depends_on: vec![2.into()],
            },
        ];

        for items in items.into_iter().permutations(3) {
            let sorted = items.into_iter().sort_by_dependencies();

            assert!(matches!(sorted[0].object_id.value.unwrap(), 2));
            assert!(matches!(sorted[1].object_id.value.unwrap(), 3));
            assert!(matches!(sorted[2].object_id.value.unwrap(), 1));
        }
    }

    #[test]
    fn circular_dependencies() {

        let result = catch_unwind(|| {

            let items = vec![
                TestItem {
                    object_id: 1.into(),
                    depends_on: vec![3.into()],
                },
                TestItem {
                    object_id: 2.into(),
                    depends_on: vec![1.into()],
                },
                TestItem {
                    object_id: 3.into(),
                    depends_on: vec![2.into()],
                },
            ];

            items.into_iter().sort_by_dependencies();
        });

        assert!(result.is_err());

    }

    #[test]
    fn handles_empty_input() {
        let items: Vec<TestItem> = vec![];
        let sorted = items.into_iter().sort_by_dependencies();
        assert_eq!(sorted, vec![]);
    }

    #[test]
    fn handles_single_item_input() {
        let items: Vec<TestItem> = vec![
            TestItem {
                object_id: 1.into(),
                depends_on: vec![],
            },];
        let sorted = items.into_iter().sort_by_dependencies();
        assert_eq!(sorted, vec![
            TestItem {
                object_id: 1.into(),
                depends_on: vec![],
            },]);
    }

}