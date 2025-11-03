import logging
import json
import time
import pandas as pd
import openai
from typing import List, Dict, Any, BinaryIO, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

class RAGPipeline:
    def __init__(self, api_key: str, model: str = "text-embedding-3-large", db_url: str = None):
        self.client = openai.OpenAI(api_key = api_key)
        self.model = model

        if not db_url:
            raise ValueError("DATABASE_URL is required for RAGPipeline")

        self.engine = create_engine(
            db_url,
            pool_size = 5,
            max_overflow = 10,
            pool_pre_ping = True,
            connect_args = {"options": "-c statement_timeout=5000"}
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        logger.info("pgvector enabled: using Postgres for vector storage and search")

    def process_csv_for_rag(self, csv_file: BinaryIO, max_rows: int = 200000) -> List[Dict[str, Any]]:
        """
        Process CSV file and create row representations with embeddings
        No hardcoded chunk sizes or health metrics
        """
        try:
            # Read CSV
            df = pd.read_csv(csv_file)
            if len(df) > max_rows:
                df = df.head(max_rows)
            logger.info(f"Processing CSV with {len(df)} rows and {len(df.columns)} columns")

            # Create row representations dynamically
            row_representations = []
            for index, row in df.iterrows():
                # Create text representation dynamically from whatever columns exist
                row_parts = []
                for col in df.columns:
                    if not pd.isna(row[col]):
                        row_parts.append(f"{col}: {row[col]}")

                row_text = ", ".join(row_parts)

                # Generate embedding for this row
                embedding = self.embed_text(row_text)

                row_representations.append({
                    'row_index': index,
                    'text': row_text,
                    'embedding': embedding,
                    'row_data': row.to_dict()
                })

            logger.info(f"Created {len(row_representations)} row representations with embeddings")
            return row_representations

        except Exception as e:
            logger.error(f"Error processing CSV for RAG: {e}")
            raise

    def embed_text(self, text: str) -> List[float]:
        """Generate embedding for text using OpenAI"""
        try:
            response = self.client.embeddings.create(
                model=self.model,
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            raise

    def embed_question(self, question: str) -> List[float]:
        """Generate embedding for user question"""
        return self.embed_text(question)

    def find_relevant_data_pgvector(self, user_question: str, user_id: str, dataset_id: Optional[str] = None, top_k: int = 50) -> List[Dict[str, Any]]:
        """
        Find relevant data using pgvector for fast similarity search
        """
        try:
            # Embed the user question
            question_embedding = self.embed_question(user_question)

            # Convert to PostgreSQL array format
            embedding_array = f"[{','.join(map(str, question_embedding))}]"

            # Use pgvector's cosine distance for similarity search
            query = text(
                """
                SET LOCAL ivfflat.probes = 4;
                SELECT row_data, 1 - (embedding <=> :embedding) as similarity
                FROM health_data_embeddings
                WHERE user_id = :user_id
                  AND (:dataset_id IS NULL OR dataset_id = :dataset_id)
                ORDER BY embedding <=> :embedding
                LIMIT :top_k
                """
            )

            with self.SessionLocal() as session:
                start = time.time()
                result = session.execute(query, {
                    'embedding': embedding_array,
                    'user_id': user_id,
                    'dataset_id': dataset_id,
                    'top_k': top_k
                })

                relevant_rows = []
                for row in result:
                    relevant_rows.append({
                        'row_data': row.row_data,
                        'similarity': row.similarity
                    })

                logger.info(f"Found {len(relevant_rows)} relevant rows using pgvector in {int((time.time()-start)*1000)}ms")
                return relevant_rows

        except Exception as e:
            logger.error(f"Error in pgvector search: {e}")
            raise

    def extract_relevant_data(self, relevant_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract the actual data from relevant rows
        Convert to JSON-serializable format
        """
        try:
            extracted_data = []

            for row_info in relevant_rows:
                row_data = row_info['row_data']

                # Convert pandas Series to regular dict if needed
                if hasattr(row_data, 'to_dict'):
                    row_data = row_data.to_dict()

                extracted_data.append(row_data)

            logger.info(f"Extracted {len(extracted_data)} rows of relevant data")
            return extracted_data

        except Exception as e:
            logger.error(f"Error extracting relevant data: {e}")
            raise

    def process_question_with_rag(self, user_question: str, user_id: str, csv_file: Optional[BinaryIO] = None, dataset_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Complete RAG pipeline using pgvector only. If a CSV is provided, (re)ingest
        embeddings for this user (and optional dataset) before querying.
        """
        try:
            if csv_file is not None:
                self.store_embeddings_in_db(csv_file, user_id=user_id, dataset_id=dataset_id)

            relevant_rows = self.find_relevant_data_pgvector(user_question, user_id=user_id, dataset_id=dataset_id)
            extracted_data = self.extract_relevant_data(relevant_rows)
            return extracted_data
        except Exception as e:
            logger.error(f"Error in RAG pipeline: {e}")
            raise

    def store_embeddings_in_db(self, csv_file: BinaryIO, user_id: str, dataset_id: Optional[str] = None, table_name: str = "health_data_embeddings"):
        """
        Store CSV embeddings in PostgreSQL with pgvector for future fast searches
        """
        try:
            if not self.use_pgvector:
                logger.warning("pgvector not configured, skipping database storage")
                return

            # Process CSV and get embeddings
            row_representations = self.process_csv_for_rag(csv_file)

            # Create table if it doesn't exist
            create_table_sql = text(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    dataset_id TEXT NULL,
                    row_index INTEGER,
                    text_content TEXT,
                    embedding vector(3072),
                    row_data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Create vector index for fast similarity search
            create_index_sql = text(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_embedding
                ON {table_name}
                USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
                CREATE INDEX IF NOT EXISTS idx_{table_name}_user_id
                ON {table_name} (user_id);
            """)

            with self.SessionLocal() as session:
                # Create table and index
                session.execute(create_table_sql)
                session.execute(create_index_sql)

                # Replace existing data for this user/dataset only
                session.execute(text(f"DELETE FROM {table_name} WHERE user_id = :user_id AND (:dataset_id IS NULL OR dataset_id = :dataset_id)"), {
                    'user_id': user_id,
                    'dataset_id': dataset_id
                })

                # Insert new embeddings
                insert_sql = text(f"""
                    INSERT INTO {table_name} (user_id, dataset_id, row_index, text_content, embedding, row_data)
                    VALUES (:user_id, :dataset_id, :row_index, :text_content, :embedding, :row_data)
                """)

                for row_rep in row_representations:
                    session.execute(insert_sql, {
                        'user_id': user_id,
                        'dataset_id': dataset_id,
                        'row_index': row_rep['row_index'],
                        'text_content': row_rep['text'],
                        'embedding': row_rep['embedding'],
                        'row_data': json.dumps(row_rep['row_data'])
                    })

                session.commit()
                logger.info(f"Stored {len(row_representations)} embeddings in database")

        except Exception as e:
            logger.error(f"Error storing embeddings in database: {e}")
            raise

