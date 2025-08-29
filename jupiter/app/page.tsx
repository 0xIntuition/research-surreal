'use client';

import { useState, useEffect } from 'react';
import { getSurrealDB } from '@/lib/surrealdb';

export default function Home() {
  const [atomCount, setAtomCount] = useState<number>(0);
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;

    const fetchAtomCount = async () => {
      try {
        const db = await getSurrealDB();
        setIsConnected(true);

        // Get atom count
        const result = await db.query<[{ count: number }[]]>(`select count() from atomcreated group all;`);
        console.log({ result })
        const atomCountValue = result[0]?.[0]?.count || 0;

        if (mounted) {
          setAtomCount(atomCountValue);
        }

      } catch (err) {
        console.error('Failed to fetch atom count:', err);
        if (mounted) {
          setError(err instanceof Error ? err.message : 'Failed to connect');
        }
      }
    };

    fetchAtomCount();

    return () => {
      mounted = false;
    };
  }, []);

  if (error) {
    return (
      <div className="min-h-screen bg-gray-100 flex items-center justify-center">
        <div className="bg-red-50 border border-red-200 rounded-lg p-6">
          <h1 className="text-2xl font-bold text-red-700 mb-2">Connection Error</h1>
          <p className="text-red-600">{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100">
      <div className="container mx-auto px-4 py-8">
        <header className="text-center mb-8">
          <h1 className="text-4xl font-bold text-gray-800 mb-2">Jupiter</h1>
          <p className="text-gray-600">Atom Count Dashboard</p>
          <div className="mt-4">
            <span className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium ${isConnected
              ? 'bg-green-100 text-green-800'
              : 'bg-yellow-100 text-yellow-800'
              }`}>
              <div className={`w-2 h-2 rounded-full mr-2 ${isConnected ? 'bg-green-400' : 'bg-yellow-400'
                }`}></div>
              {isConnected ? 'Connected to SurrealDB' : 'Connecting...'}
            </span>
          </div>
        </header>

        <div className="flex justify-center">
          <div className="bg-white rounded-lg shadow-lg p-8 hover:shadow-xl transition-shadow max-w-sm w-full">
            <h3 className="text-xl font-semibold text-gray-700 mb-4 text-center">
              Atoms Created
            </h3>
            <div className="text-5xl font-bold text-blue-600 text-center mb-2">
              {atomCount.toLocaleString()}
            </div>
            <p className="text-sm text-gray-500 text-center">Total atoms</p>
          </div>
        </div>

        {atomCount === 0 && isConnected && (
          <div className="text-center py-8 mt-8">
            <div className="text-gray-400 text-lg">No atoms found in the database</div>
            <p className="text-gray-500 mt-2">Waiting for atoms to be created...</p>
          </div>
        )}

        <footer className="mt-12 text-center text-sm text-gray-500">
          <p>Data fetched from SurrealDB</p>
        </footer>
      </div>
    </div>
  );
}
