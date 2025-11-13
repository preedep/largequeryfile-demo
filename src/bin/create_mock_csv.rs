use csv::Writer;
use fake::faker::address::en::*;
use fake::faker::name::en::*;
use fake::Fake;
use rand::Rng;
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
    
    let file = File::create("mock_data.csv")?;
    let mut writer = Writer::from_writer(file);
    
    // Write header
    writer.write_record(&["first_name", "last_name", "citizen_id", "address"])?;

    // Generate 1,000,000 rows
    for i in 0..1_000_000 {
        let first_name: String = FirstName().fake();
        let last_name: String = LastName().fake();
        let citizen_id = generate_citizen_id();
        let address: String = StreetName().fake();
        
        writer.write_record(&[first_name, last_name, citizen_id, address])?;
        
        // Progress indicator
        if (i + 1) % 10_000 == 0 {
            info!("Generated {} rows...", i + 1);
        }
    }
    
    writer.flush()?;
    info!("Successfully generated mock_data.csv with 1,000,000 rows!");

    Ok(())
}