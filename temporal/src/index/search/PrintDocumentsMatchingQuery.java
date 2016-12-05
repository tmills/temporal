package index.search;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class PrintDocumentsMatchingQuery {
	
	public static void main(String[] args) throws CorruptIndexException, IOException {

		final int maxHits = 100;
		final String field = "content";
		final String query = "lasted minutes";
		
  	DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get("/home/dima/data/mimic/index/")));
  	IndexSearcher indexSearcher = new IndexSearcher(indexReader);
  	
    PhraseQuery.Builder queryBuilder = new PhraseQuery.Builder();
    for(String word : query.split(" ")) {
    	queryBuilder.add(new Term(field, word));
    }
  	queryBuilder.setSlop(2); 
  	
  	TopDocs topDocs = indexSearcher.search(queryBuilder.build(), maxHits);
  	ScoreDoc[] scoreDocs = topDocs.scoreDocs;
  	
  	for(ScoreDoc scoreDoc : scoreDocs) {
  		Document doc = indexSearcher.doc(scoreDoc.doc);
  		String text = doc.get(field);
  		System.out.println(text);
  		System.out.println();
  	}
  	
  	indexReader.close();
	}
}

