use itertools::Itertools;
use crate::*;
use crate::storage::{CopyDestination, CopySource, DataFormat};



#[derive(Debug, Default)]
pub struct CopyDataOptions {
    /// Force this data format to be used
    pub data_format: Option<DataFormat>,
}

pub async fn copy_data(source: &impl CopySource, destination: &mut impl CopyDestination, options: CopyDataOptions) -> Result<()> {

    let data_format = get_data_type(source, destination, &options).await?;


    let definition = source.get_introspection().await?;

    destination.apply_structure(&definition).await?;


    for schema in &definition.schemas {
        for table in &schema.tables {

            let data = source.get_data(schema, table, &data_format).await?;

            assert_eq!(data_format, data.get_data_format());

            destination.apply_data(schema, table, data).await?;
        }
    }

    destination.apply_post_structure(&definition).await?;

    Ok(())
}

async fn get_data_type(source: &impl CopySource, destination: &impl CopyDestination, options: &CopyDataOptions) -> Result<DataFormat> {
    let source_formats = source.supported_data_format().await?;
    let destination_formats = destination.supported_data_format().await?;

    let overlap = source_formats.iter().filter(|f| destination_formats.contains(f)).collect_vec();

    if overlap.is_empty() || options.data_format.as_ref().is_some_and(|d| !overlap.contains(&d)) {
        Err(ElefantToolsError::DataFormatsNotCompatible {
            supported_by_source: source_formats,
            supported_by_target: destination_formats,
            required_format: options.data_format.clone(),
        })
    } else {
        for format in &overlap {
            if let DataFormat::PostgresBinary {..} = format {
                return Ok((*format).clone());
            }
        }



        Ok(overlap[0].clone())
    }
}

