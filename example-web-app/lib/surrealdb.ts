import { Surreal } from 'surrealdb';

let db: Surreal | null = null;

export async function getSurrealDB(): Promise<Surreal> {
  if (db) {
    return db;
  }

  db = new Surreal();
  
  const surrealUrl = process.env.SURREAL_URL || 'ws://localhost:8000/rpc';
  
  try {
    // Connect to the database
    await db.connect(surrealUrl);
    
    // Select namespace and database
    await db.use({
      namespace: process.env.SURREAL_NS || 'rindexer',
      database: process.env.SURREAL_DB || 'i7n_surreal'
    });
    
    // Sign in with root credentials
    await db.signin({
      username: process.env.SURREAL_USER || 'root',
      password: process.env.SURREAL_PASS || 'root'
    });
    
    console.log('Connected to SurrealDB');
    return db;
  } catch (error) {
    console.error('Failed to connect to SurrealDB:', error);
    throw error;
  }
}

export { db };