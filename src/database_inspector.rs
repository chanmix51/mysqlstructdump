use sqlx::mysql::MySqlPool;
use sqlx::prelude::*;
use futures::executor::block_on;
use std::fmt::{self, Formatter, Display};

pub struct DatabaseInspector {
    pool: MySqlPool,
}

#[derive(Debug,sqlx::FromRow)]
pub struct TableList {
    table_name: String,
    table_type: String,
    table_rows: Option<u32>,
    index_length: Option<u32>,
    auto_increment: Option<u8>,
}

impl Display for TableList {
   fn fmt(&self, f: &mut Formatter) -> fmt::Result { 
       let table_type = match self.table_type.as_str() {
            "BASE TABLE"    => "TABLE",
            "VIEW"          => "VIEW",
            _               => "UNKNOWN",
            };

        let table_rows = match self.table_rows {
            Some(o) => format!("{:5}", o),
            None    => format!("{:>5}", "no"),
        };

        let index_length = match self.index_length {
            Some(o) => format!("{:12}", o),
            None    => format!("{:>12}", "no"),
        };

        let auto_increment = match self.auto_increment {
            Some(1)     => "Y".to_string(),
            _           => "N".to_string(),
        };
        
       write!(f, "{:5} {:>15} | {} rows | {} bytes | {}", table_type, self.table_name, table_rows, index_length, auto_increment)
   }
}

impl DatabaseInspector {
    pub fn new(url: &str) -> DatabaseInspector {
        let pool = block_on(MySqlPool::new(url)).unwrap();

        DatabaseInspector { pool }
    }

    pub fn get_tables(&self) -> Vec<TableList> {
        let sql = r"
    select
        TABLE_NAME      as  table_name,
        TABLE_TYPE      as table_type,
        TABLE_ROWS      as table_rows,
        INDEX_LENGTH    as index_length,
        AUTO_INCREMENT  as auto_increment
    from information_schema.tables
    where table_schema=?
        ";

        block_on(sqlx::query_as::<_, TableList>(sql)
            .bind("akeneo_pim")
            .fetch_all(&self.pool)
            ).unwrap()
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_table_inspector() {
        let inspector = DatabaseInspector::new("mysql://root:root@mysql.lxc/akeneo_pim");
        let result = inspector.get_tables();
        let tables = result.as_slice();

        assert_eq!("chu".to_string(), tables[0].table_name);
        assert_eq!("VIEW".to_string(), tables[1].table_type);
        assert_eq!(Some(1), tables[2].auto_increment);
    }
}