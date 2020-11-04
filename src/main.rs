mod database_inspector;

use database_inspector::DatabaseInspector;

fn main() {
    let inspector = DatabaseInspector::new("mysql://root:root@mysql.lxc/akeneo_pim");
    for table in inspector.get_tables() {
        println!("{}", table);
    }
}