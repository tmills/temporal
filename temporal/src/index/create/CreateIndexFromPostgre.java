package index.create;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class CreateIndexFromPostgre {

	public static void main(String[] args) throws IOException, SQLException {

		// connect to postgresql and get the data
		Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/MIMIC2", "dima", "dima");
		connection.setAutoCommit(false); // need this so that setFetchSize() below has an effect
		String query = "select text from mimic2v26.noteevents";
		Statement statement = connection.createStatement();
		statement.setFetchSize(1000); // fetching the entire table results in out-of-memory error
		ResultSet resultSet = statement.executeQuery(query);
		
		// set up lucene index
		Directory directory = FSDirectory.open(Paths.get("/home/dima/data/mimic/index/"));
		Analyzer analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
		IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
		
		// now write to index
		while(resultSet.next()) {
			String text = resultSet.getString(1);
			Document document = new Document();
			document.add(new TextField("content", text, Field.Store.YES));
			indexWriter.addDocument(document);
		}
		
		connection.close();
		indexWriter.close();
		System.out.println("done!");
	}
}
