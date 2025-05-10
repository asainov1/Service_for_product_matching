use std::collections::HashSet;

/// Calculate Jaccard Similarity between two product titles
fn jaccard_similarity(s1: &str, s2: &str) -> f64 {
    let set1: HashSet<&str> = s1.split_whitespace().collect();
    let set2: HashSet<&str> = s2.split_whitespace().collect();

    let intersection: HashSet<_> = set1.intersection(&set2).collect();
    let union: HashSet<_> = set1.union(&set2).collect();

    if union.len() == 0 {
        return 0.0;
    }

    intersection.len() as f64 / union.len() as f64
}

fn main() {
    let product1 = "Samsung Galaxy S21 Ultra 5G";
    let product2 = "Samsung Galaxy S21 5G Smartphone";

    let similarity = jaccard_similarity(product1, product2);
    println!("Similarity: {:.2}", similarity);

    if similarity > 0.5 {
        println!("Products are similar.");
    } else {
        println!("Products are not similar.");
    }
}
