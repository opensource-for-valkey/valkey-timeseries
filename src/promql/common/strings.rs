use std::cmp::Ordering;

/// Compare two strings.
/// Source: https://crates.io/crates/alphanumeric-sort
pub fn compare_str_alphanumeric<A: AsRef<str>, B: AsRef<str>>(a: A, b: B) -> Ordering {
    let mut c1 = a.as_ref().chars();
    let mut c2 = b.as_ref().chars();

    // this flag is to handle something like "1點" < "1-1點"
    let mut last_is_number = false;

    let mut v1: Option<char> = None;
    let mut v2: Option<char> = None;

    loop {
        let ca = {
            match v1.take() {
                Some(c) => c,
                None => match c1.next() {
                    Some(c) => c,
                    None => {
                        return if v2.take().is_some() || c2.next().is_some() {
                            Ordering::Less
                        } else {
                            Ordering::Equal
                        };
                    }
                },
            }
        };

        let cb = {
            match v2.take() {
                Some(c) => c,
                None => match c2.next() {
                    Some(c) => c,
                    None => {
                        return Ordering::Greater;
                    }
                },
            }
        };

        let b_zero = f64::from(b'0');

        if ca.is_ascii_digit() && cb.is_ascii_digit() {
            let mut da = f64::from(ca as u32) - b_zero;
            let mut db = f64::from(cb as u32) - b_zero;

            // this counter is to handle something like "001" > "01"
            let mut dc = 0isize;

            for ca in c1.by_ref() {
                if ca.is_ascii_digit() {
                    da = da * 10.0 + (f64::from(ca as u32) - b_zero);
                    dc += 1;
                } else {
                    v1 = Some(ca);
                    break;
                }
            }

            for cb in c2.by_ref() {
                if cb.is_ascii_digit() {
                    db = db * 10.0 + (f64::from(cb as u32) - b_zero);
                    dc -= 1;
                } else {
                    v2 = Some(cb);
                    break;
                }
            }

            last_is_number = true;

            let ordering = da.total_cmp(&db);
            if ordering != Ordering::Equal {
                return ordering;
            } else {
                match dc.cmp(&0) {
                    Ordering::Equal => {}
                    Ordering::Greater => return Ordering::Greater,
                    Ordering::Less => return Ordering::Less,
                }
            }
        } else {
            match ca.cmp(&cb) {
                Ordering::Equal => last_is_number = false,
                Ordering::Greater => {
                    return if last_is_number && (ca > (255 as char)) ^ (cb > (255 as char)) {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    };
                }
                Ordering::Less => {
                    return if last_is_number && (ca > (255 as char)) ^ (cb > (255 as char)) {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    };
                }
            }
        }
    }
}
