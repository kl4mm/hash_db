pub enum Command<'a> {
    Insert(&'a str, &'a str),
    Delete(&'a str),
    Get(&'a str),
    None,
}

impl<'a> Command<'a> {
    pub fn from_str(s: &'a str) -> Self {
        let split: Vec<&str> = s.split_whitespace().collect();

        if split.len() < 2 || split.len() > 3 {
            return Command::None;
        }

        match split[0].to_lowercase().as_str() {
            "insert" => {
                if split.len() != 3 {
                    return Command::None;
                }

                Command::Insert(split[1], split[2])
            }
            "delete" => {
                if split.len() != 2 {
                    return Command::None;
                }

                Command::Delete(split[1])
            }
            "get" => {
                if split.len() != 2 {
                    return Command::None;
                }

                Command::Get(split[1])
            }
            _ => Command::None,
        }
    }
}
