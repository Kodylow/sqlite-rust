use anyhow::Result;
use tracing::info;

#[derive(Debug)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub sql: String,
}

#[derive(Debug)]
pub struct ColumnDef {
    pub name: String,
    pub column_type: String,
}

impl TableSchema {
    pub fn parse(name: String, sql: String) -> Result<Self> {
        info!("Parsing schema for table '{}': {}", name, sql);

        // Extract column definitions
        let columns = if let Some(start_idx) = sql.find('(') {
            // Get everything between first ( and last )
            let end_idx = sql.rfind(')').unwrap_or(sql.len());
            let columns_str = &sql[start_idx + 1..end_idx];

            info!("Parsing columns: {}", columns_str);

            // Split on commas and parse each column definition
            columns_str
                .split(',')
                .filter_map(|col| {
                    let col = col.trim();
                    if col.is_empty() {
                        return None;
                    }

                    // Split on whitespace and get column name and type
                    let parts: Vec<&str> = col.split_whitespace().collect();
                    if parts.is_empty() {
                        return None;
                    }

                    let name = parts[0].trim_matches('"').to_string();
                    let col_type = parts.get(1).map_or("".to_string(), |s| s.to_string());

                    info!("Found column: {} (type: {})", name, col_type);

                    Some(ColumnDef {
                        name,
                        column_type: col_type,
                    })
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok(TableSchema { name, columns, sql })
    }
}
