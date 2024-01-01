use std::io::BufRead;
use std::ops::Deref;
use std::vec;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{pin_mut, Stream, StreamExt};
use itertools::Itertools;
use tokio::fs::File;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use crate::models::PostgresDatabase;
use crate::models::SimplifiedDataType;
use crate::models::PostgresSchema;
use crate::models::PostgresTable;
use crate::storage::{BaseCopyTarget, CopyDestination, DataFormat, TableData};
use crate::Result;

pub struct SqlFile<F: AsyncWrite + Unpin + Send + Sync> {
    file: F,
}

impl<F: AsyncWrite + Unpin + Send + Sync> SqlFile<F> {
    pub async fn new(path: &str) -> Result<SqlFile<BufWriter<File>>> {
        let file = File::create(path).await?;
        Ok(SqlFile {
            file: BufWriter::new(file),
        })
    }
}

#[async_trait]
impl<F: AsyncWrite + Unpin + Send + Sync> BaseCopyTarget for SqlFile<F> {
    async fn supported_data_format(&self) -> Result<Vec<DataFormat>> {
        Ok(vec![DataFormat::Text])
    }
}

#[async_trait]
impl<F: AsyncWrite + Unpin + Send + Sync> CopyDestination for SqlFile<F> {
    async fn apply_structure(&mut self, db: &PostgresDatabase) -> Result<()> {
        let file = &mut self.file;

        for schema in &db.schemas {
            let sql = schema.get_create_statement();
            file.write_all(sql.as_bytes()).await?;

            file.write_all("\n".as_bytes()).await?;

            for table in &schema.tables {
                let sql = table.get_create_statement(schema);
                file.write_all(sql.as_bytes()).await?;

                file.write_all("\n".as_bytes()).await?;
            }
        }

        file.flush().await?;

        Ok(())
    }

    async fn apply_data<S: Stream<Item=Result<Bytes>> + Send>(&mut self, schema: &PostgresSchema, table: &PostgresTable, data: TableData<S>) -> Result<()> {
        let file = &mut self.file;

        let column_types = table.columns.iter().map(|c| c.get_simplified_data_type()).collect_vec();

        file.write_all(b"insert into ").await?;
        file.write_all(schema.name.as_bytes()).await?;
        file.write_all(b".").await?;
        file.write_all(table.name.as_bytes()).await?;
        file.write_all(b" (").await?;
        let mut first = true;
        for column in &table.columns {
            if first {
                first = false;
            } else {
                file.write_all(b", ").await?;
            }
            file.write_all(column.name.as_bytes()).await?;
        }
        file.write_all(b")").await?;
        file.write_all(b" values").await?;

        file.write_all(b"\n").await?;

        let stream = data.into_stream();

        pin_mut!(stream);

        let mut first = true;
        while let Some(bytes) = stream.next().await {
            match bytes {
                Ok(bytes) => {
                    eprintln!("raw bytes: {:?}", bytes);
                    if first {
                        first = false;
                    } else {
                        file.write_all(b",\n").await?;
                    }

                    let bytes: &[u8] = bytes.deref();
                    let bytes = &bytes[0..bytes.len() - 1];
                    let cols = BufRead::split(bytes, b'\t').zip(column_types.iter());
                    file.write_all(b"(").await?;
                    let mut first = true;
                    for (bytes, col_data_type) in cols {
                        if first {
                            first = false;
                        } else {
                            file.write_all(b", ").await?;
                        }
                        match bytes {
                            Ok(bytes) => {

                                if bytes == [b'\\', b'N'] {
                                    file.write_all(b"null").await?;
                                    continue;
                                }


                                match col_data_type {
                                    SimplifiedDataType::Number => {
                                        match bytes[..] {
                                            [b'N', b'a', b'N'] | [b'I', b'n', b'f', b'i', b'n', b'i', b't', b'y'] | [b'-', b'I', b'n', b'f', b'i', b'n', b'i', b't', b'y'] => {
                                                file.write_all(b"'").await?;
                                                file.write_all(&bytes).await?;
                                                file.write_all(b"'").await?;
                                            }
                                            _ => {
                                                file.write_all(&bytes).await?;
                                            }
                                        }
                                    }
                                    SimplifiedDataType::Text => {
                                        file.write_all(b"E'").await?;
                                        if bytes.contains(&b'\'') {

                                            let s = std::str::from_utf8(&bytes).unwrap();
                                            let s = s.replace('\'', "''");
                                            file.write_all(s.as_bytes()).await?;

                                        } else {
                                            file.write_all(&bytes).await?;
                                        }
                                        file.write_all(b"'").await?;
                                    }
                                    SimplifiedDataType::Bool => {
                                        let value = bytes[0] == b't';
                                        file.write_all(format!("{}", value).as_bytes()).await?;
                                    }
                                }
                            }
                            Err(e) => panic!("wtf: {:?}", e)
                        }
                    }
                    file.write_all(b")").await?;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        file.write_all(b";\n\n").await?;

        file.flush().await?;

        Ok(())
    }

    async fn apply_post_structure(&mut self, db: &PostgresDatabase) -> Result<()> {
        for schema in &db.schemas {
            for table in &schema.tables {
                for index in &table.indices {
                    let sql = index.get_create_index_command(schema, table);
                    self.file.write_all(sql.as_bytes()).await?;
                    self.file.write_all("\n".as_bytes()).await?;
                }

            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use super::*;
    use crate::test_helpers::*;
    use tokio::test;
    use crate::copy_data::{copy_data, CopyDataOptions};
    use crate::storage::postgres_instance::PostgresInstanceStorage;

    async fn export_to_string(source: &TestHelper) -> String {

        let mut result_file = Vec::<u8>::new();

        {
            let mut sql_file = SqlFile {
                file: &mut result_file,
            };

            let source = PostgresInstanceStorage::new(source.get_conn()).await.unwrap();


            copy_data(&source, &mut sql_file, CopyDataOptions::default()).await.unwrap();
        }

        String::from_utf8(result_file).unwrap()
    }


    #[test]
    async fn exports_to_fake_file() {
        let source = get_test_helper().await;

        //language=postgresql
        source.execute_not_query(r#"
        create table people(
            id serial primary key,
            name text not null,
            age int not null check (age > 0),
            constraint multi_check check (name != 'fsgsdfgsdf' and age < 9999)
        );

        create index people_age_idx on people (age desc) where (age % 2 = 0);
        create index people_age_brin_idx on people using brin (age);

        insert into people(name, age)
        values
            ('foo', 42),
            ('bar', 89),
            ('nice', 69),
            (E'str\nange', 420),
            (E't\t\tap', 421),
            (E'q''t', 12)
            ;
        "#).await;


        let result_file = export_to_string(&source).await;

        similar_asserts::assert_eq!(result_file, indoc! {r#"
            create schema if not exists public;
            create table public.people (
                id integer not null,
                name text not null,
                age integer not null,
                constraint people_pkey primary key (id),
                constraint multi_check check (((name <> 'fsgsdfgsdf'::text) AND (age < 9999))),
                constraint people_age_check check ((age > 0))
            );

            insert into public.people (id, name, age) values
            (1, E'foo', 42),
            (2, E'bar', 89),
            (3, E'nice', 69),
            (4, E'str\nange', 420),
            (5, E't\t\tap', 421),
            (6, E'q''t', 12);

            create index people_age_brin_idx on public.people using brin (age);
            create index people_age_idx on public.people using btree (age desc nulls first) where (age % 2) = 0;
            "#});

        let destination = get_test_helper().await;
        destination.execute_not_query(&result_file).await;


        let items = destination.get_results::<(i32, String, i32)>("select id, name, age from people;").await;

        assert_eq!(items, vec![
            (1, "foo".to_string(), 42),
            (2, "bar".to_string(), 89),
            (3, "nice".to_string(), 69),
            (4, "str\nange".to_string(), 420),
            (5, "t\t\tap".to_string(), 421),
            (6, "q't".to_string(), 12),
        ]);
    }


    #[test]
    async fn edge_case_values_floats() {
        let source = get_test_helper().await;

        //language=postgresql
        source.execute_not_query(r#"
        create table edge_case_values(
            r4 float4,
            r8 float8
        );

        insert into edge_case_values(r4, r8)
        values (1.0, 1.0),
               ('NaN', 'NaN'),
               ('Infinity', 'Infinity'),
               ('-Infinity', '-Infinity'),
               (null, null);

        "#).await;

        let result_file = export_to_string(&source).await;

        similar_asserts::assert_eq!(result_file, indoc! {r#"
            create schema if not exists public;
            create table public.edge_case_values (
                r4 real,
                r8 double precision
            );

            insert into public.edge_case_values (r4, r8) values
            (1, 1),
            ('NaN', 'NaN'),
            ('Infinity', 'Infinity'),
            ('-Infinity', '-Infinity'),
            (null, null);

            "#});

        let destination = get_test_helper().await;
        destination.execute_not_query(&result_file).await;


        let items = destination.get_results::<(Option<f32>, Option<f64>)>("select r4, r8 from edge_case_values;").await;

        assert_eq!(items.len(), 5);
        assert_eq!(items[0], (Some(1.0), Some(1.0)));
        assert_eq!(items[2], (Some(f32::INFINITY), Some(f64::INFINITY)));
        assert_eq!(items[3], (Some(f32::NEG_INFINITY), Some(f64::NEG_INFINITY)));
        assert_eq!(items[4], (None, None));

        let nan_tuple = items[1];

        assert!(nan_tuple.0.unwrap().is_nan());
        assert!(nan_tuple.1.unwrap().is_nan());
    }
}