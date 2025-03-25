use bytes::BufMut;

#[derive(Clone, Debug)]
pub enum Command {
    PUT { key: i32, val: i32 },
    GET { key: i32 },
    DELETE { key: i32 },
    // TODO: load, range, stats
}

impl Command {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Self::PUT { key, val } => {
                buf.put_u8(b'p');
                buf.put_i32(*key);
                buf.put_i32(*val);
            }
            Self::GET { key } => {
                buf.put_u8(b'g');
                buf.put_i32(*key);
            }
            Self::DELETE { key } => {
                buf.put_u8(b'd');
                buf.put_i32(*key);
            }
        }
    }

    pub fn from_input(input: &str) -> Option<Self> {
        let mut split_iter = input.split(' ');
        let tag = split_iter.next()?;

        match tag {
            "p" => {
                let key: i32 = split_iter.next()?.parse().ok()?;
                let val: i32 = split_iter.next()?.parse().ok()?;
                Some(Command::PUT { key, val })
            }
            "g" => {
                let key: i32 = split_iter.next()?.parse().ok()?;
                Some(Command::GET { key })
            }
            "d" => {
                let key: i32 = split_iter.next()?.parse().ok()?;
                Some(Command::DELETE { key })
            }
            _ => None,
        }
    }
}
