use std::time::Duration;

use mongodb::bson::doc;
use mongodb::options::{ClientOptions, IndexOptions};
use mongodb::IndexModel;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use crate::error::SpanErr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListObjectTokens {
    pub start_after: String,
    pub created_at: mongodb::bson::DateTime,
    pub consumed_at: Option<mongodb::bson::DateTime>,
}

pub struct MongoDB {
    pub client: mongodb::Client,
    pub db: mongodb::Database,

    pub list_object_tokens: mongodb::Collection<ListObjectTokens>,
}

impl MongoDB {
    #[instrument(name = "mongodb/connect", skip_all)]
    pub async fn connect(
        uri: String,
        db_name: String,
    ) -> Result<MongoDB, SpanErr<mongodb::error::Error>> {
        let client_options = ClientOptions::parse(uri).await?;
        let client = mongodb::Client::with_options(client_options)?;
        let db = client.database(db_name.as_str());
        info!("Connected to MongoDB ({}).", db_name);

        let mongo = Self {
            client,
            list_object_tokens: db.collection("list_object_tokens"),
            db,
        };

        info!("Creating indexes...");

        mongo
            .list_object_tokens
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "created_at": 1 })
                    .options(
                        IndexOptions::builder()
                            .expire_after(Duration::from_days(1))
                            .build(),
                    )
                    .build(),
            )
            .await?;

        info!("list_object_tokens created_at index created.");

        mongo
            .list_object_tokens
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "consumed_at": 1 })
                    .options(
                        IndexOptions::builder()
                            .expire_after(Duration::from_mins(10))
                            .build(),
                    )
                    .build(),
            )
            .await?;

        info!("list_object_tokens consumed_at index created.");

        info!("Indexes created.");

        Ok(mongo)
    }
}
