use crate::command::{TranslateType, Translater as Base};

pub struct Translater;

impl Base for Translater {
    fn translate(&self, t: TranslateType) -> String {
        match t {
            TranslateType::Unknown(v) => v,
            TranslateType::Email => "Please enter a valid email address.".to_owned(),
            TranslateType::Required => "This field is required.".to_owned(),
            TranslateType::Phone => "Please enter a valid phone number.".to_owned(),
            TranslateType::LengthMin(min) => format!("Please enter at least {min} characters."),
            TranslateType::LengthMax(max) => format!("Please enter no more than {max} characters."),
            TranslateType::LengthMinMax(min, max) => {
                format!("The input should be between {min} and {max} characters long.")
            }
            TranslateType::LengthEqual(equal) => {
                format!("Please enter exactly {equal} characters.")
            }
        }
    }
}
