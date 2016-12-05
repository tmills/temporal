package index.search;

import java.io.IOException;
import java.nio.file.Paths;

import javax.swing.JOptionPane;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import utils.Utils;

public class SearchUtility {
	
	public static void main(String[] args) throws CorruptIndexException, IOException {

		final int maxHits = 5000;
		final String searchField = "content";
		final String indexLocation = "/home/tmill/mnt/rc-pub/resources/mimic/index";

		String queryText = JOptionPane.showInputDialog("Enter query");
		
  	DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexLocation)));
  	IndexSearcher indexSearcher = new IndexSearcher(indexReader);

  	PhraseQuery.Builder phraseQueryBuilder = new PhraseQuery.Builder();
  	for(String word : queryText.split(" ")) {
  		phraseQueryBuilder.add(new Term(searchField, word));
  	}
  	phraseQueryBuilder.setSlop(0);
  	
  	TopDocs topDocs = indexSearcher.search(phraseQueryBuilder.build(), maxHits);
  	ScoreDoc[] scoreDocs = topDocs.scoreDocs;  		

  	for(ScoreDoc scoreDoc : scoreDocs) {
  		Document document = indexSearcher.doc(scoreDoc.doc);
  		String text = document.get(searchField).toLowerCase().replace('\n', ' ');
  		String context = Utils.getContext(queryText, text, 140);
  		System.out.println(context);
  	}
  	
  	indexReader.close();
  	System.out.println("total hits: " + scoreDocs.length);
	}
}

