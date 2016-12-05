package utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;

public class Utils {

  /**
   * Make a phrase query from a phrase.
   */
  public static PhraseQuery makePhraseQuery(String phrase, String field, int slop) {
    
    PhraseQuery.Builder queryBuilder = new PhraseQuery.Builder();
    for(String word : phrase.split(" ")) {
      queryBuilder.add(new Term(field, word));
    }
    queryBuilder.setSlop(slop);
    
    return queryBuilder.build();
  }
  
  /**
   * Get context for a string. Return "" if string not found in text.
   * 
   * TODO: Occasionally no context is found when the indexer removed certain
   * characters which still exist in the source text. E.g. when "... pain, and swelling"
   * is in the source document, the query "pain and swelling" will return this document.
   * However, this method will not find the occurence of "pain and swelling" in the
   * document because of the comma.
   */
  public static String getContext(String string, String text, int characterWindow) {
    
    String noEOL = text.replace('\n', ' ');
    int begin = noEOL.indexOf(string);
    if(begin == -1) {
      return "";
    }
    
    int end = begin + string.length();
    int contextBegin = Math.max(0, begin - characterWindow);
    int contextEnd = Math.min(text.length(), end + characterWindow);
    
    return noEOL.substring(contextBegin, contextEnd);
  }

  /**
   * Print documents in a one-line format that match the text of the query.
   */
  public static void printMatchingDocuments(
      String queryText, 
      ScoreDoc[] scoreDocs, 
      IndexSearcher indexSearcher, 
      String field) 
          throws CorruptIndexException, IOException {
    
    for(ScoreDoc scoreDoc : scoreDocs) {
      Document doc = indexSearcher.doc(scoreDoc.doc);
      String text = doc.get(field).replace('\n', ' ');
      System.out.println("* " + queryText);
      System.out.println("* " + text);
    }
  }

  /**
   * Read values of a set from file (one entry per line).
   */
  public static Set<String> readSetValuesFromFile(String path) throws FileNotFoundException {
    
    Set<String> values = new HashSet<String>();
    Scanner scanner = new Scanner(new File(path));
    
    while(scanner.hasNextLine()) {
      String line = scanner.nextLine();
      values.add(line);
    }
    
    return values;
  }
  
  /**
   * Prepare for dumping stuff to file.  
   */
  public static BufferedWriter getWriter(String filePath, boolean append) {

    BufferedWriter bufferedWriter = null;
    try {
      FileWriter fileWriter = new FileWriter(filePath, append);
      bufferedWriter = new BufferedWriter(fileWriter);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return bufferedWriter;
  }
}
