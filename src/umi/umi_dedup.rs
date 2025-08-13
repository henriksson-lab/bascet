use std::collections::HashMap;

use super::KMER2bit;

<<<<<<< HEAD
=======

>>>>>>> main
pub struct OneUMI {
    umi: u32,
    cnt: u32,
    //parent: Option<usize>, //Only useful if we want to keep track of the tree of UMIs. Typically not
}

<<<<<<< HEAD
pub struct UMIcounter {}
impl UMIcounter {
    ///////////////////////////////
    /// Prepare UMI algorithm from a list of UMI strings
    pub fn prepare_from_str(input_list: &[Vec<u8>]) -> Vec<OneUMI> {
        //Encode all UMIs
        let encoded: Vec<u32> = input_list
            .iter()
            .map(|x| unsafe { KMER2bit::encode_u32(x.as_slice()) })
            .collect();
=======

pub struct UMIcounter {
}
impl UMIcounter {

    ///////////////////////////////
    /// Prepare UMI algorithm from a list of UMI strings
    pub fn prepare_from_str(
        input_list: &[Vec<u8>]
    ) -> Vec<OneUMI> {

        //Encode all UMIs
        let encoded: Vec<u32> = input_list.iter().map(
            |x|  unsafe { KMER2bit::encode_u32(x.as_slice()) }
        ).collect();
>>>>>>> main

        //Get frequency of each UMI
        let map_encoded_cnt = count_element_function(encoded);

<<<<<<< HEAD
        //Keep frequencies in a list
        let mut list_encoded_cnt: Vec<OneUMI> = map_encoded_cnt
            .iter()
            .map(|(&umi, &cnt)| OneUMI {
                umi: umi,
                cnt: cnt as u32,
                //parent: None
            })
            .collect();

        //Sort list, smallest to greatest
        list_encoded_cnt.sort_by(|a, b| a.cnt.cmp(&b.cnt));
=======
        Self::prepare_from_map(&map_encoded_cnt)
        /*
        //Keep frequencies in a list
        let mut list_encoded_cnt: Vec<OneUMI> = map_encoded_cnt.iter().map(
            |(&umi, &cnt)| OneUMI {
                umi: umi,
                cnt: cnt as u32,
                //parent: None
            }
        ).collect();

        //Sort list, smallest to greatest
        list_encoded_cnt.sort_by(|a,b| a.cnt.cmp(&b.cnt));

        list_encoded_cnt
         */
        
    }


    pub fn prepare_from_map(
        map_encoded_cnt: &HashMap<u32,u32>
    ) -> Vec<OneUMI> {

        //Keep frequencies in a list
        let mut list_encoded_cnt: Vec<OneUMI> = map_encoded_cnt.iter().map(
            |(&umi, &cnt)| OneUMI {
                umi: umi,
                cnt: cnt as u32,
                //parent: None
            }
        ).collect();

        //Sort list, smallest to greatest
        list_encoded_cnt.sort_by(|a,b| a.cnt.cmp(&b.cnt));
>>>>>>> main

        list_encoded_cnt
    }

    ///////////////////////////////
    /// Deduplicate using directional algorithm
<<<<<<< HEAD
    pub fn directional_algorithm(list_umi: &mut Vec<OneUMI>, max_distance: u32) -> u32 {
        let mut total_cnt = 0;
=======
    pub fn directional_algorithm(
        list_umi: &Vec<OneUMI>, 
        max_distance: u32
    ) -> u32 {

        let mut total_cnt=0;
>>>>>>> main

        //For each UMI
        for i in 0..list_umi.len() {
            let this_umi = list_umi.get(i).unwrap().umi;
            let mut this_cnt = 1;

            //scan UMIs of higher count (lower in list) if any of them are similar.
            //best to assign counts to UMI with most counts in case of tie
<<<<<<< HEAD
            'find_parent: for j in (i + 1)..list_umi.len() {
                let other_umi = list_umi.get(j).unwrap().umi;
                let hamming_dist = (this_umi ^ other_umi).count_ones();
=======
            'find_parent: for j in (i+1)..list_umi.len() {

                let other_umi = list_umi.get(j).unwrap().umi;
                let hamming_dist = (this_umi^other_umi).count_ones();
>>>>>>> main

                //Possible todo: if the size of the list is small then it might be better to compute min hamming distance (SIMD parallelized),
                //then just do a single comparison at the end

                //https://doc.rust-lang.org/std/simd/num/trait.SimdInt.html#tymethod.reduce_min
                // i32x4::from_array([1, 2, 3, 4]).reduce_min();

                if hamming_dist <= max_distance {
                    //list_umi.get_mut(i).unwrap().parent = Some(j); //If we want to track tree structure
                    this_cnt = 0;
                    break 'find_parent;
                }
            }

            total_cnt += this_cnt;
        }

        total_cnt
<<<<<<< HEAD
    }
}

///////////////////////////////
/// Get frequency of each element as a hashmap
fn count_element_function<I>(it: I) -> HashMap<I::Item, usize>
=======

    }



}



///////////////////////////////
/// Get frequency of each element as a hashmap
fn count_element_function<I>(it: I) -> HashMap<I::Item, u32>
>>>>>>> main
where
    I: IntoIterator,
    I::Item: Eq + core::hash::Hash,
{
    let mut result = HashMap::new();

    for item in it {
        *result.entry(item).or_insert(0) += 1;
    }

    result
}

<<<<<<< HEAD
=======









>>>>>>> main
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_umi_2() {
<<<<<<< HEAD
        let mut lst = Vec::new();
=======

        let mut lst=Vec::new();
>>>>>>> main
        lst.push("ATCGATCG".as_bytes().to_vec());
        lst.push("ATCGATCG".as_bytes().to_vec());
        lst.push("ATCGATCG".as_bytes().to_vec());
        lst.push("ATCGATCG".as_bytes().to_vec());
        lst.push("ATCGATCG".as_bytes().to_vec());

        lst.push("ATCGATCC".as_bytes().to_vec()); //1bp from above
        lst.push("ATCGATCC".as_bytes().to_vec());

        lst.push("ATTGATCC".as_bytes().to_vec()); //1bp from above

        lst.push("AAAGATCC".as_bytes().to_vec()); //quite different

        let mut prep = UMIcounter::prepare_from_str(lst.as_slice());

        let cnt = UMIcounter::directional_algorithm(&mut prep, 1);

        println!("umis {}", cnt);

        assert_eq!(cnt, 2);
<<<<<<< HEAD
    }

    #[test]
    fn count_umi_0() {
        let mut lst = Vec::new();
=======

    }


    #[test]
    fn count_umi_0() {

        let mut lst=Vec::new();
>>>>>>> main
        lst.push("ATCGATCG".as_bytes().to_vec());
        lst.push("ATCGATCG".as_bytes().to_vec());
        lst.push("ATCGATCG".as_bytes().to_vec());
        lst.push("ATCGATCG".as_bytes().to_vec());
        lst.push("ATCGATCG".as_bytes().to_vec());

        lst.push("ATCGATCC".as_bytes().to_vec()); //1bp from above
        lst.push("ATCGATCC".as_bytes().to_vec());

        lst.push("ATTGATCC".as_bytes().to_vec()); //1bp from above

        lst.push("AAAGATCC".as_bytes().to_vec()); //quite different

        let mut prep = UMIcounter::prepare_from_str(lst.as_slice());

        let cnt = UMIcounter::directional_algorithm(&mut prep, 0);

        println!("umis {}", cnt);

        assert_eq!(cnt, 4);
<<<<<<< HEAD
    }
}
=======

    }



}
>>>>>>> main
