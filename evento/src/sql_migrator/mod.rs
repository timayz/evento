use sqlx_migrator::{Info, Migrator};

mod m0001;
mod m0002;

pub use m0001::InitMigration;
pub use m0002::M0002;

pub fn new<DB: sqlx::Database>() -> Result<Migrator<DB>, sqlx_migrator::Error>
where
    InitMigration: sqlx_migrator::Migration<DB>,
    M0002: sqlx_migrator::Migration<DB>,
{
    let mut migrator = Migrator::default();
    migrator.add_migration(Box::new(InitMigration))?;
    migrator.add_migration(Box::new(M0002))?;

    Ok(migrator)
}
