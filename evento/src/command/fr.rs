use crate::command::{TranslateType, Translater as Base};

pub struct Translater;

impl Base for Translater {
    fn translate(&self, t: TranslateType) -> String {
        match t {
            TranslateType::Unknown(v) => v,
            TranslateType::Email => "Veuillez entrer une adresse e-mail valide.".to_owned(),
            TranslateType::Required => "Ce champ est obligatoire.".to_owned(),
            TranslateType::Phone => "Veuillez entrer un numéro de téléphone valide.".to_owned(),
            TranslateType::LengthMin(min) => format!("Veuillez entrer au moins {min} caractères."),
            TranslateType::LengthMax(max) => format!("Veuillez ne pas dépasser {max} caractères."),
            TranslateType::LengthMinMax(min, max) => {
                format!("La saisie doit comporter entre {min} et {max} caractères.")
            }
            TranslateType::LengthEqual(equal) => {
                format!("Veuillez entrer exactement {equal} caractères.")
            }
        }
    }
}
