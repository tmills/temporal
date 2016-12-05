package durations;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import utils.Utils;

/**
 * Extract durations of sign/symptom(s).
 * 
 * TODO: what if a document contains multiple instances of a pattern? are all used?
 */

public class SignSymptomDurations {
	
	public static void main(String[] args) throws CorruptIndexException, IOException {
    if(args.length < 2){
      System.err.println("Two required arguments: <Event file (one per line)> <Lucene index directory>");
      System.exit(-1);
    }

		final int maxHits = 1000000;
		final String searchField = "content";
		final String indexLocation = args[1];
		final String signAndSymptomFile = args[0];
		final String outputDirectory = "/home/tmill/Projects/duration_mining/temporal/output/disease_disorders/";
		final int contextWindowInCharacters = 140;
		final List<String> durationIndicators = Arrays.asList("for", "x");
		
    DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexLocation)));
    IndexSearcher indexSearcher = new IndexSearcher(indexReader);

		for(String symptom : Utils.readSetValuesFromFile(signAndSymptomFile)) {
      String outputFile = outputDirectory + symptom + ".txt";
      BufferedWriter writer = Utils.getWriter(outputFile, false);
      
		  for(String durationIndicator : durationIndicators) {
		    String queryText = symptom + " " + durationIndicator;
		    PhraseQuery phraseQuery = Utils.makePhraseQuery(queryText, searchField, 0);

		    TopDocs topDocs = indexSearcher.search(phraseQuery, maxHits);
		    ScoreDoc[] scoreDocs = topDocs.scoreDocs;  		

		    for(ScoreDoc scoreDoc : scoreDocs) {
		      Document document = indexSearcher.doc(scoreDoc.doc);
		      String text = document.get(searchField).toLowerCase().replace("[\n\r]", " ");      // TODO: lowercase or not?
		      String context = Utils.getContext(queryText, text, contextWindowInCharacters);
		      String contextWithNonPrintableCharactersRemoved = context.replaceAll("[^\\x20-\\x7E]", ""); // allowable range in hex
		      writer.write(contextWithNonPrintableCharactersRemoved + "\n");
		    }
		  }
		  
      writer.close();
		}
		
    indexReader.close();
    System.out.println("done!");
	}
}

