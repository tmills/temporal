package index.create;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class CreateIndexFromCsv {

  public static void main(String[] args) throws IOException {
    if(args.length < 2){
      System.err.println("Two required arguments: <CSV file path> <Index directory>");
      System.exit(-1);
    }

    Directory directory = FSDirectory.open(Paths.get(args[1]));
    Analyzer analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
    IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);

    File csvFile = new File(args[0]);
    for(CSVRecord record : CSVFormat.EXCEL.withHeader().parse(new FileReader(csvFile))){
      String text = record.get("TEXT");
      String subj = record.get("SUBJECT_ID");
      String row = record.get("ROW_ID");
      String hadm = record.get("HADM_ID");
      String category = record.get("CATEGORY");
      
      Document document = new Document();
      document.add(new TextField("content", text, Field.Store.YES));
      document.add(new StringField("subject_id", subj, Field.Store.YES));
      document.add(new StringField("row_id", row, Field.Store.YES));
      document.add(new StringField("hadm_id", hadm, Field.Store.YES));
      document.add(new StringField("category", category, Field.Store.YES));

      indexWriter.addDocument(document); 
    }

    indexWriter.close();
    System.out.println("done!");
  }
}
