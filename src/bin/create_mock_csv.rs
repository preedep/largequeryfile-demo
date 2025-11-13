use csv::Writer;
use fake::faker::address::en::*;
use fake::faker::name::en::*;
use fake::Fake;
use rand::Rng;
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use log::info;

fn generate_citizen_id() -> String {
    let mut rng = rand::rng();
    format!(
        "{}-{}-{}-{}-{}",
        rng.random_range(1..=9),
        rng.random_range(1000..=9999),
        rng.random_range(10000..=99999),
        rng.random_range(10..=99),
        rng.random_range(0..=9)
    )
}

fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    info!("Starting CSV generation...");
    
    // HashSets to ensure uniqueness
    let mut citizen_ids = HashSet::new();
    let mut full_names = HashSet::new();
    let mut citizen_id_list = Vec::new();
    
    let file = File::create("mock_data.csv")?;
    let mut writer = Writer::from_writer(file);
    
    // Write header
    writer.write_record(&["first_name", "last_name", "citizen_id", "address"])?;

    // Generate 500,000 rows with unique citizen_id and full_name
    let mut generated = 0;
    while generated < 500_000 {
        let first_name: String = FirstName().fake();
        let last_name: String = LastName().fake();
        let full_name = format!("{} {}", first_name, last_name);
        
        // Check if full_name is unique
        if !full_names.insert(full_name.clone()) {
            continue; // Skip if duplicate
        }
        
        // Generate unique citizen_id
        let mut citizen_id = generate_citizen_id();
        while !citizen_ids.insert(citizen_id.clone()) {
            citizen_id = generate_citizen_id();
        }
        
        let address: String = StreetName().fake();
        
        writer.write_record(&[&first_name, &last_name, &citizen_id, &address])?;
        citizen_id_list.push(citizen_id);
        
        generated += 1;
        
        // Progress indicator
        if generated % 10_000 == 0 {
            info!("Generated {} rows...", generated);
        }
    }
    
    writer.flush()?;
    info!("Successfully generated mock_data.csv with 500,000 unique rows!");
    info!("Total unique citizen_ids: {}", citizen_ids.len());
    info!("Total unique full_names: {}", full_names.len());

    // Generate salary dataset
    info!("Starting salary dataset generation...");
    let salary_file = File::create("mock_data_salary.csv")?;
    let mut salary_writer = Writer::from_writer(salary_file);
    
    // Write header
    salary_writer.write_record(&["citizen_id", "salary"])?;
    
    let mut rng = rand::rng();
    for (i, citizen_id) in citizen_id_list.iter().enumerate() {
        // Generate salary between 15,000 and 150,000
        let salary = rng.random_range(15_000..=150_000);
        salary_writer.write_record(&[citizen_id, &salary.to_string()])?;
        
        // Progress indicator
        if (i + 1) % 10_000 == 0 {
            info!("Generated {} salary rows...", i + 1);
        }
    }
    
    salary_writer.flush()?;
    info!("Successfully generated mock_data_salary.csv with 500,000 rows!");

    Ok(())
}