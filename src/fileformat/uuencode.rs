
use std::collections::HashSet;

/*
Formatting of string

x => U<unicode number in hex>_  
U => U55_
“ “ => U20_
“-” => U2d_


TODO: should one prefix with XX: or something, to make it clear that encoding has been performed?


TODO: functions below have not yet been tested

*/



use lazy_static::lazy_static;
lazy_static! {
    static ref VALID_CHARS: HashSet<char> = {
        let mut m = HashSet::new();
        for c in ("0123456789_ABCDEFGHIJKLMNOPQRSTVWXYZabcdefghijklmnopqrstuv" as &str).chars() {
            m.insert(c);
        }
        m
    };
}


pub fn convert_to_uuencode(s: String) -> Vec<u8> {

    //Alternative is to go through string and count size needed!
    let mut out: Vec<u8> = Vec::with_capacity(s.len()*3); 
    //let mut utf8: [u8;4];
    for c in s.chars() {
        if VALID_CHARS.contains(&c){
            out.push(c as u8);
        } else {
            let utf8 = c as u32; //char is 4 bytes 
            out.push(b'U');
            out.extend_from_slice(format!("{utf8:X}").as_bytes()); //Can likely be made faster
            out.push(b'_');
            // this is similar to https://doc.rust-lang.org/std/primitive.char.html#method.escape_unicode
        }
    }
    out
}





pub fn convert_from_uuencode(s: Vec<u8>) -> String {

    let mut out: Vec<char> = Vec::with_capacity(s.len()); 

    let mut i=0;
    loop {
        let c = s[i];
        if c == b'U' {
            //Set start position
            let from = i;

            //Find end position at _
            i+=1;
            while s[i]!=b'_' {
                i+=1;
            }

            let hex_part = &s[from..i];

            let hex_string = String::from_utf8_lossy(hex_part);
            let z = u32::from_str_radix(&hex_string, 16).expect(format!("Failed to interpret {:?} as a hex number, decoding uuencode", &hex_part).as_str());

            unsafe {
                out.push(
                    char::from_u32_unchecked(z)
                );
            }
            
            //Skip the _
            i+=1;

        } else {
            out.push(
                c as char
            );
            i+=1;
        }

        if i==out.len() {
            break;
        }
    }

    //Assemble string. This function can likely be made much faster if an ut8 array is assembled directly instead of vec<char>, as char is u32 and requires conversion
    out.into_iter().collect()
}

