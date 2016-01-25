import java.util.*;
import java.io.*;;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.text.SimpleDateFormat;
import java.text.DateFormat;


public class IndexBuilder
{
	// used to hold all the files from the directory and reading the directory which has the files in it
	private ArrayList<String> fileNames = new ArrayList<String>();
	private final String dirName = "New_York/";
	private HashMap<Integer,Document> urlTable = new HashMap<Integer,Document>(); // used for mapping of docIds and urls
	private HashMap<String,InvertedIndex> invertedIndex = new HashMap<String,InvertedIndex>(); // used to hold the index


	// these variables are used to index the documents
	private int docId = 1; 


	public static void main(String [] args) throws Exception
	{
		IndexBuilder program = new IndexBuilder();
		// String fileName = "NY_2001.htm";
		// org.jsoup.nodes.Document doc = Jsoup.parse(new File("New_York/"+ fileName),"UTF-8");
		// String st = (doc.getElementById("mw-revision-date").text());
		// DateFormat formatter = new SimpleDateFormat("HH:mm, d MMM yyyy");
		// Date date = (Date)formatter.parse(st);
		// //formatter = new SimpleDateFormat(date);
		// System.out.println(formatter.format(date));
		program.run(args);
	}

	private void run(String [] args) throws IOException
	{
		// get all the files in the directory
		readAllFiles();
		
		// loop through and index each document
		for (String fileName : fileNames)
		{
			// send the file over to JSOUP to parse it
			//System.out.println(dirName+fileName);
			org.jsoup.nodes.Document doc = Jsoup.parse(new File(dirName + fileName),"UTF-8");
			// this basically gets all the text in the p tags and replaces all non characters with spaces. 
			String bodyText = doc.body().getElementsByTag("p").text().replaceAll("[^a-zA-Z ]","");

			// get the document date that is in the span under the id mw-revision-date
			String date = (doc.getElementById("mw-revision-date").text());
			//DateFormat formatter = new SimpleDateFormat("HH:mm, d MMM yyyy");

		
			// put the document in the URLTable so we can map back to it during the queryExecuter stage
			urlTable.put(docId,new Document(fileName,bodyText.length(),date)); 
			
			// now loop through each word and create a invertedIndex for it
			for (String word : bodyText.split(" "))
			{
				// check if this word exists
				//System.out.println("in here " + word);
				if (invertedIndex.containsKey(word))
				{
					// ck if this document already exists
					// if it does, simply incremenet the frequency
					// if it doesnt exist, add the document to the list
					//System.out.println("inheee");
					InvertedIndex temp = invertedIndex.get(word);
					if (temp.doesDocumentExist(docId))
						temp.incrementFreq();
					else
						temp.addDocument(docId,date);
				}
				else
				{
					// if it doesnt exist, then create an entry for it
					//System.out.println("otherwissse");
					invertedIndex.put(word,new InvertedIndex(docId,date));
				}
			}


			// before we go back to the top, increment the docId
			docId++;
		}


		// write the file to OutPut Directory

		
		PrintWriter write = new PrintWriter("index.txt");

		for (Map.Entry<String,InvertedIndex> entry : invertedIndex.entrySet())
		{
			write.println(entry.getKey() +  entry.getValue());
		}
		write.close();

		// write the urlTable to the disk
		write = new PrintWriter("urlTable.txt");
		for (Map.Entry<Integer,Document> doc : urlTable.entrySet())
		{
			Document tmp = doc.getValue();
			// write to the file. Contains the docUrl,then date, then the Doc length and then the key
			write.println(tmp.docUrl+ ";" + tmp.date + ";" + tmp.length + ";" + doc.getKey());
		}
			
	
		write.close();

	}

	/*
	* This method reads all the files to be indexed from the directory
	*/
	private void readAllFiles() throws IOException
	{
		// open the directory and list all the files
		File directory = new File(dirName);
		File [] listOfFiles = directory.listFiles();
		
		// loop through each file and check if it really is a file, if so add it to the array
		for (File file:listOfFiles)
		{
			if (file.isFile())
				fileNames.add(file.getName());
		}
	}


/*------------------------------------------------------------------------------------
	- This is used to store the list for the for each doc id
	// this class is used for the URLTable
	// this class holds the document url and the Document Length
	// eveyrthing is public in this case for convience 

	// this inner class is used to put togther the word listings
-------------------------------------------------------------------------------------*/

	class InvertedIndex{

		// used to hold the list for each document
		private ArrayList<InvertedList> list;

		public InvertedIndex()
		{
			list = new ArrayList<InvertedList>();
		}
		public InvertedIndex(int docId,String date)
		{
			list = new ArrayList<InvertedList>();
			list.add(new InvertedList(docId,1,date));

		}

		// this creates a new document posting in the index
		public void addDocument(int docId,String date)
		{
			// when we first create the document, the freqency is one
			list.add(new InvertedList(docId,1,date));
		}

		// check if the current document is already added
		public boolean doesDocumentExist(int docId)
		{
			return list.get(list.size()-1).getId() == docId;
		}

		// increase the frequency
		public void incrementFreq()
		{
			// increase the frequency of the last document added since this is the document we are still parsing
			list.get(list.size()-1).incrementFreq();
		}

		// overriding the toString method to print the data to the file
		public String toString()
		{
			StringBuilder builder = new StringBuilder();

			// loop through each element of the array and output it
			for(InvertedList l : list )
				builder.append(l.toString());

			return builder.toString();


		}
	}

/*------------------------------------------------------------------------------------
	- This is used to store the list for the for each doc id
-------------------------------------------------------------------------------------*/
	// this innner class is used to create the inverted List for each posting
	class InvertedList{

		// variables that every document has
		private int docId;
		private int freq;
		private String date;

		public InvertedList(int docId, int freq, String date)
		{
			this.docId = docId;
			this.freq = freq;
			this.date = date;
		}

		// this method increments the freq
		public void incrementFreq(){ freq++; }

		public int getId(){return docId;}

		// overridding the toString method
		public String toString()
		{
			return  ";" + docId + ";" + freq + ";" + date;
		}
	}

/*------------------------------------------------------------------------------------
	- This is used to store the list for the for each doc id
	// this class is used for the URLTable
	// this class holds the document url and the Document Length
	// eveyrthing is public in this case for convience 
-------------------------------------------------------------------------------------*/
	class Document{
		public String docUrl;
		public int length;
		public String date;

		public Document(String docUrl, Integer length,String date)
		{
			this.docUrl = docUrl;
			this.length = length;
			this.date = date;
		}
	}
}
