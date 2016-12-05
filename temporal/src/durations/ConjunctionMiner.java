package durations;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import utils.Utils;

/**
 * TODO: it seems like only the first occurence in a document of a query is returned.
 * TODO: probably need to get all of them.
 */
public class ConjunctionMiner {
	
	public static void main(String[] args) throws CorruptIndexException, IOException {

		final int maxHits = 1000000;
		final String searchField = "content";
		final String indexLocation = "/home/dima/data/mimic/index/";
		final String signAndSymptomFile = "/home/dima/thyme/duration/data/unique-sign-symptoms.txt";
		final String outputFile = "/home/dima/out/conjunction/counts.txt";
		final String dotFile = "/home/dima/out/conjunction/graph.dot";
		final int minCoocurence = 5; // discard conjunctions below this frequency threshold
		
		BufferedWriter writer = Utils.getWriter(outputFile, false);
    DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexLocation)));
    IndexSearcher indexSearcher = new IndexSearcher(indexReader);

    Set<String> signsAndSymptoms = Utils.readSetValuesFromFile(signAndSymptomFile);

    // set of co-occuring symptom pairs, e.g. {{pain, anxiety}, {abuse, depression}, ...}
    Set<Set<String>> adjacency = new HashSet<Set<String>>();
    
    for(String ss1 : signsAndSymptoms) {
		  for(String ss2 : signsAndSymptoms) {
		    if(ss1.equals(ss2)) {
		      continue;
		    }
		    
		    String queryText = ss1 + " and " + ss2;
		    PhraseQuery phraseQuery = Utils.makePhraseQuery(queryText, searchField, 0);
		    TopDocs topDocs = indexSearcher.search(phraseQuery, maxHits);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        
        if(scoreDocs.length > minCoocurence) {
          writer.write(queryText + ": " + scoreDocs.length + "\n");
          adjacency.add(new HashSet<String>(Arrays.asList(ss1, ss2)));
        }
		  }
		}
		
    toDot(adjacency, dotFile);
    
		writer.close();
    indexReader.close();
	}

	/**
	 * Convert set of co-occuring symptoms into graphviz dot format.
	 */
	public static void toDot(Set<Set<String>> adjacency, String file) throws IOException {

	  BufferedWriter writer = Utils.getWriter(file, false);
	  writer.write("graph g {\n");

	  for(Set<String> pair : adjacency) {
	    String output = String.format("%s--%s;\n", pair.toArray()[0], pair.toArray()[1]);
	    writer.write(output);
	  }

	  writer.write("}\n");
	  writer.close();
	}
}
