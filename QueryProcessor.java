import java.util.*;
import java.io.*;
import java.lang.*;
import java.text.SimpleDateFormat;
import java.text.DateFormat;



public class QueryProcessor{

	private final String urlTableFileName = "urlTable.txt";
	private final String invertedIndexFileName = "index.txt";
	private final String userBasedRankingFileName = "userBasedRank.txt";
	private final String currentEventFileName = "currentEventsRank.txt";

	// data structures to hold the inverted index and urlTable
	private HashMap<String,InvertedIndex> index = new HashMap<String,InvertedIndex>();
	private HashMap<Integer,Document > urlTable = new HashMap<Integer,Document >();

	// these variables will be used for calculating the BM25 score
	private int totalDocumentLength = 0;

	// heap to hold the top k 
	DataComparator comp = new DataComparator();
	private PriorityQueue<Pair<Integer,Double> > heap =  new PriorityQueue<Pair<Integer,Double> >(comp); // used to keep the top K documents

	// used to store the documents in its ranking order and will be used when the user makes a selection on the document
	// private ArrayList<Pair<Integer,Double>> documentReturnList = new ArrayList<Pair<Integer,Double>>();

	// HashMap used for user click based ranking
	private HashMap<String,UserRank> userBasedRankTable = new HashMap<String,UserRank>();
	private int documentClickFreq = 0; // keep track of how many users clicked on the document
	private int documentClickCount = 0; // keep track how many documents were clicked all together

	// This is used for the current event ranking
	private HashMap<String,Pair<Integer,Date>> currentRankTable = new HashMap<String,Pair<Integer,Date>>();
	private final int DAYSTHRESHOLD = 7; // number of days we should check for the current rank search
	private final int NUMOFCLICKS  = 10; // number of searches for this day that we need
	private final int DAYSRANGE = 30; // number of days this search should be within
	private final int MILLIS_PER_DAY = 86400000;



	public static void main(String [] args) throws Exception
	{
		QueryProcessor program = new QueryProcessor();
		program.run();
		// Scanner in = new Scanner(System.in);

		// String t = in.nextLine();
		// String [] val = t.split(" ");
		// for (String s:val)
		// {
		// 	System.out.println(s);
		// }

		// in.close();
	}


	private void run() throws Exception
	{
		// read the data structures that are in hard drive into memory
		readUrlTable();
		readInvertedIndex();
		readUserRankIndex(); // used to rank documents based on what user's selected
		readCurrentSearch(); // this determines if it is a current events search

		// now run the query processor
		queryProcessor();
	}
//--------------------------------------------------------------------------------------------------------------File Reading/Writing


/*------------------------------------------------------------------------------------------------------
		-- This is used for checking if this is a current Events search
		-- Basically check what queries were searched the most in say previous week
		-- If the queries are above a threshold (say 10 for this project), and they were all searched within a week (7days), 
		   then they must be a current event search ( since alot of users would be checking the most current events)
-------------------------------------------------------------------------------------------------------*/
	private void readCurrentSearch()
	{
		try{
			Scanner reader = new Scanner(new File(currentEventFileName));
			while (reader.hasNext())
			{
				// read the whole line. Basically this is how the format of the file will be
				// Query;DATE;number of times searched
				String line = reader.nextLine();
				String [] arr = line.split(";");
				String query = arr[0];
				int count = Integer.parseInt(arr[1]);
				DateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZ yyyy",Locale.ENGLISH);
				Date date = (Date)formatter.parse(arr[2]);
				// insert the data in the hashtable
				currentRankTable.put(query,new Pair<Integer,Date>(count,date));
			}
			reader.close();
		}
		catch (Exception e)
		{
			System.out.println("Error in readCurrentSearch:"+e);
		}
	}
/*------------------------------------------------------------------------------------------------------
	-- Write the current events data to the disk 
----------------------------------------------------------------------------------------------------------*/
	private void writeCurrentSearch()
	{
		try{
			PrintWriter write = new PrintWriter(currentEventFileName);
			for (Map.Entry<String,Pair<Integer,Date>> rank : currentRankTable.entrySet())
			{
				Pair<Integer,Date> tmp = rank.getValue();
				write.println(rank.getKey()+";"+tmp.first+";"+tmp.second);
			}
			write.close();

		}
		catch(Exception e)
		{
			System.out.println("Error in writing current search data"+e);
		}
	}

/*-----------------------------------------------------------------------------------------------------
		- This is used for reading a index in from the hard drive which is used for user based ranking
------------------------------------------------------------------------------------------------------*/
	private void readUserRankIndex() 
	{

		try{
			Scanner reader = new Scanner(new File(userBasedRankingFileName));
			while (reader.hasNext())
			{
				// The file is structured this way: QUERY <space> DOCUMENT ID  <space> # of Times selected
				String line = reader.nextLine();
				String [] arr = line.split(":");
				String query = arr[0];
				UserRank userrank = new UserRank();
				// loop through and keep adding each list
				for (int i = 1; i < arr.length; ++i)
				{				
					int docid =  Integer.parseInt(arr[i++]);
					int length = Integer.parseInt(arr[i]);
					userrank.addRank(docid,length);
					documentClickFreq+= length;
					documentClickCount++; // increment the document count
				}

				userBasedRankTable.put(query, userrank);
				System.out.println(userrank);
			}
			reader.close();
		}
		catch (Exception e)
		{
			System.out.println("ERROR OCCURED IN readUserRankIndex "+ e);
		}	
	}
/*-----------------------------------------------------------------------------------------------------
		- This is used for writing the user based ranking to the file
------------------------------------------------------------------------------------------------------*/
	private void  writeUserBasedRanking()
	{
		try{
			PrintWriter write = new PrintWriter(userBasedRankingFileName);
			for (Map.Entry<String,UserRank> rank : userBasedRankTable.entrySet())
			{
				//System.out.println(rank.getKey()+":"+rank.getValue()+":");
				write.println(rank.getKey()+rank.getValue());
			}
			write.close();
		}
		catch(Exception e){
			System.out.println("ERROR IN WRITING USER BASED RANKING"+e);
		}
	}

	/*
	* Reads the file for the URLTable and fills the URL in main memory since it is small (~20 documents)
	*/
	private void readUrlTable() throws Exception
	{
		try{
			Scanner reader = new Scanner(new File(urlTableFileName));

			// loop through each 
			while (reader.hasNext())
			{
				// read the whole line and parse the line
				// the line format is document name;document last date modified;Document Size;Document ID
				// split based of : (semi-colon)
				String line = reader.nextLine(); 
				String [] arr = line.split(";");
				// for (String s:arr)
				// 	System.out.println(s);
				// now here's how the array is layed out:
				// 0 - document name
				// 1- date + time
				// 2- Size
				// 3- ID

				// parse the string for the date, since we need to format it
				DateFormat formatter = new SimpleDateFormat("HH:mm, d MMM yyyy");
				Date date = (Date)formatter.parse(arr[1]);
				// put this document in the url table
				int docid = Integer.parseInt(arr[3]);
				String docname = arr[0];
				int length = Integer.parseInt(arr[2]);
				urlTable.put(docid,new Document(docname,date,length));

				// sum the document length
				totalDocumentLength+= length;
				//Pair tmp = new Pair(docId,new Pair(urlName,length));
			}

			// close the reader
			reader.close();

		}

		catch(IOException e)
		{
			System.out.println("ERROR in reading the URLTable file " + e);
		}
	}

	/*
	* This method reads the invertedIndex
	*/
	private void readInvertedIndex() throws Exception
	{
		try{

			Scanner reader = new Scanner(new File(invertedIndexFileName));
			
			// loop through and read every intance of the inverted index
			while (reader.hasNextLine())
			{
				// inverted list for this string
				InvertedIndex tmpIndex = new InvertedIndex();
				// read the docId, freq and Date for all the documents this word has
				String line = reader.nextLine();
				//System.out.println(line);
				// split the string based on the space
				// the first element is DocID, 
				// second is freq
				// third is year
				String [] arr = line.split(";");
				String word = arr[0].toLowerCase();
				for (int i = 1; i < arr.length;++i)
				{
					// create a inverted index which we will ad r
					int docid = Integer.parseInt(arr[i]);
					Integer freq = Integer.parseInt(arr[++i]);
					//parse the date
					DateFormat formatter = new SimpleDateFormat("HH:mm, d MMM yyyy");
					Date date = (Date)formatter.parse( arr[++i]);
					tmpIndex.addDocument(docid,freq,date);
				}
				// insert the document to the hashMap
				index.put(word,tmpIndex);
			}
			reader.close();
		}
		catch(IOException e)
		{
			System.out.println("ERROR in reading the index into main memory " + e);
		}
	}

// -------------------------------------------------------------------------------------------------------------- Query Processing & Ranking
	/*
	* This method will take a user input and return the top k documents
	*/
	private void queryProcessor() throws Exception
	{
		// ask and read the input given by the user
		Scanner in = new Scanner(System.in);
		// list that will contain the inverted index for all the terms
		ArrayList<InvertedIndex> list = new ArrayList<InvertedIndex>();
		boolean isResult = true;
		boolean specialSearch = false; // if its a current search or time search4
		Date date = null; // used for user entered date
		//int year = 0; // this is the year the user enters in

		while (true)
		{
			System.out.println("What would you like to search for?");
			String query = in.nextLine();
			if (query.equals("quit"))
				break;

			ArrayList<Pair<Integer,Double>> documentReturnList = new ArrayList<Pair<Integer,Double>>();
			// if it is a current search or a time specified search, set it to true
			if (query.contains("-c") || query.contains("-t"))
				specialSearch = true;

			// split the query into separate words
			String [] values = query.split(" ");


			// now for each word, we will put the invertedIndex in the list and run nextGEQ on them
			for (int i = 0; i < values.length ; ++i)
			{
				if (values[i].equals("-t"))
				{
					//the next value is the year
					// format the date
					DateFormat formatter = new SimpleDateFormat("MM/dd/yy");
					date = (Date)formatter.parse(values[i+1]);
					System.out.println(date);
					//year = Integer.parseInt(values[i+1].trim());

					++i;
				}
				// find the invertedIndex for this string and add it to the list
				else if (index.get(values[i]) == null)
				{

					isResult = false;
					break;
				}
				
				// only add it to list, if its not a parameter
				else if (!values[i].equals("-c"))
				{
					list.add(index.get(values[i]));
					index.get(values[i]).resetLastId(); // reset this lists last saved id
					System.out.println(values[i]);
				}			
			}
			// check if its a special search
			// 		if it is, send it to the right search time ( either time or current)
			if (specialSearch && list.size() > 0)
			{
				System.out.println("========" + date);
				if (date!=null)
					runDAATTime(list,date);
				else
					runDAATCurrent(list);
				// runDAATCurrent(list);
			}
			else if (checkCurrent(query)) // if this is a current events search, then return true
				runDAATCurrent(list);
			// now we have all the lists ready. We will find the intersections using DAAT traversal
			else if (list.size()>0 && isResult)
			{
				//System.out.println("Not a special search");
				rankCurrent(query); // rank the current event documents
				runDAAT(list,query);
			}

			//year = 0;
			//comp.greater = true;
			//System.out.println(heap.size());
			int result = 0;
			boolean hasResults = false; // used to keep track if this is a result, and can we take a user input for the result
			while (heap.size() > 0)
			{
				Pair<Integer,Double> temp = heap.poll();
				System.out.println(++result+") " + temp.first + " -> " + temp.second);
				// add the document for when the user makes a selection
				documentReturnList.add(temp); // this stores the results in the array, which the user can select the options
				hasResults = true; // this is a result
			}

			if (hasResults){
				System.out.println("Which result will you like to look at?");
				int userChoice = Integer.parseInt(in.nextLine());
				// this is the result the user likes and choose, so this result should get a score boost
				rankDocument(userChoice,query,documentReturnList);
			}
			else{
				System.out.println("No results found");
			}

			// reset everything
			list.clear();
			// documentReturnList.clear();
			isResult = true;
			specialSearch = false;
			date = null;
		}
		// we broke out
		in.close();
		// write to the file the userBasedRanking
		writeUserBasedRanking();
		writeCurrentSearch();
	}

/*----------------------------------------------------------------------------------------------------------
	-- Basically check if this was a current event search
-----------------------------------------------------------------------------------------------------------*/
	private boolean checkCurrent(String query)
	{
		Pair<Integer,Date> tmp = currentRankTable.get(query);
		if (tmp!=null)
		{
			// ck if this is within the month time range and if its above the count limit needed to be a current events document
			Date curr = new Date();
			int days = (int)Math.abs(curr.getTime() - tmp.second.getTime())/MILLIS_PER_DAY ;
			if (days <= DAYSRANGE && tmp.first >= NUMOFCLICKS)
				return true; // its a current events search
		}

		System.out.println("FAiled current events search");
		return false;

	}
/*----------------------------------------------------------------------------------------------------------
	-- Basically check if this query was searched within the last week
-----------------------------------------------------------------------------------------------------------*/
	private void rankCurrent(String query)
	{
		// 	check if this query was searched for within the past week, if yes update the time and increment the time
		Pair<Integer,Date> tmp = currentRankTable.get(query);
		if (tmp==null)
		{
			// create a new entry
			tmp = new Pair<Integer,Date>(1,new Date());
			currentRankTable.put(query,tmp);
		}
		else
		{
			// ck if this occurs within the last week, if yes, update the time and increment the search count
			Date curr = new Date();
			int days = (int)Math.abs(curr.getTime() - tmp.second.getTime())/ MILLIS_PER_DAY ;
			//int days = 2;

			if (days <= DAYSTHRESHOLD)
			{
				// increment the time and the date
				tmp.first = tmp.first + 1;
				tmp.second = curr;
			}
			else
			{
				// otherwise set the occurance to one and reset the time
				tmp.first = 1;
				tmp.second = curr;
			}
		}

	}

/*-----------------------------------------------------------------------------------------------------------
	Rank documents based on user score ( note used for future ranking)

	we have the file read in from the document already, so simply update the file and write it back to the disk 
	(Note we are simply increasing the times a user clicked on this result for this particular query )

*----------------------------------------------------------`---------------------------------------------------*/
	private void rankDocument(int userChoice,String query, ArrayList<Pair<Integer,Double>> documentReturnList)
	{
		//check if this exists in the hashMap, if it does we'll simply increment the count
		UserRank temp = userBasedRankTable.get(query);
		documentClickFreq++; // we will always incremeent freq
		if (temp == null)
		{
			//System.out.println(documentReturnList.get(userChoice-1).first);
			temp = new UserRank(); // make a new entry for this query with the document selected and set freq to 1
			temp.addRank(documentReturnList.get(userChoice-1).first,1);
			userBasedRankTable.put(query,temp);
			//System.out.println("=============" + query + " ==" + documentReturnList.get(userChoice-1).first );
			// incremeent document click Count + freq
			documentClickCount++;
		}
		else
			temp.addRank(documentReturnList.get(userChoice-1).first);
	} // end of rankDocument function

	/*
	* @ Param - this is the inverted list for each term. We will use these lists to find the intersections
	* This method will find the document based on the year closest to the ones passed in
	*/
	private void runDAATTime(ArrayList<InvertedIndex> list, Date date)
	{
		// sort the list first
		Collections.sort(list,new Comparator<InvertedIndex>(){

			public int compare(InvertedIndex l1, InvertedIndex l2)
			{
				return l1.getListSize() - l2.getListSize();
			}

		});

		// we want to sort in decreasing order since this is the difference of the years
		comp.greater = false;

		// for (InvertedIndex i : list)
		// 	System.out.println(i.getListSize());
		int did = 0;
		int d=-1;

		// run until we have exaughsted all the documents of the shorter list
		while (did != -1)
		{
			// get the document from the shortest list
			did = list.get(0).nextGEQ(did);

			// check if other documents have the same document ID
			for (int i = 0; ( i<list.size() && (d = list.get(i).nextGEQ(did)) == did ); ++i );

			// this is not a intersection, so assign the new did and start over
			if (d>did && did != -1) 
				did = d;
			// this is a intersection
			// compute the BM25 score
			else if (did != -1)
			{
				//double diff = (double) Math.abs(list.get(0).getYear(did) - year); // this is the same document ID, so get the year based on the current value
				Date curr = list.get(0).getDate(did);
				double diff = (double) Math.abs((curr.getTime() - date.getTime()) / (double) MILLIS_PER_DAY );
				heap.add(new Pair<Integer,Double>(did,diff));
				
				did++;// incremenet the DID
			}

		}
	}

	/*
	* @ Param - this is the inverted list for each term. We will use these lists to find the intersections
	* This method will find the document based on the most relevant year
	*/
	private void runDAATCurrent(ArrayList<InvertedIndex> list)
	{
		// sort the list first
		Collections.sort(list,new Comparator<InvertedIndex>(){

			public int compare(InvertedIndex l1, InvertedIndex l2)
			{
				return l1.getListSize() - l2.getListSize();
			}

		});

		comp.greater = false; // we want min heap in this case
		// for (InvertedIndex i : list)
		// 	System.out.println(i.getListSize());
		int did = 0;
		int d=-1;

		// run until we have exaughsted all the documents of the shorter list
		while (did != -1)
		{
			// get the document from the shortest list
			did = list.get(0).nextGEQ(did);

			// check if other documents have the same document ID
			for (int i = 0; ( i<list.size() && (d = list.get(i).nextGEQ(did)) == did ); ++i );

			// this is not a intersection, so assign the new did and start over
			if (d>did && did != -1) 
				did = d;
			// this is a intersection
			// compute the BM25 score
			else if (did != -1)
			{
				//double year = (double)list.get(0).getYear(did); // this is the same document ID, so get the year based on the current value
				Date curr = new Date(); // current time
				Date date = list.get(0).getDate(did); // get the date for this document
				double diff = (double) Math.abs((curr.getTime() - date.getTime()) / (double) MILLIS_PER_DAY );
				heap.add(new Pair<Integer,Double>(did,diff));
				
				did++;// incremenet the DID
			}
		}
	}
	/*
	* @Param list- this is the inverted List for each term. We will use these lists to find the intersections
	* This method will find all the documents and add them to the Min Heap, and return them.
	*/
	private void runDAAT(ArrayList<InvertedIndex> list,String query)
	{
		// sort the list first
		Collections.sort(list,new Comparator<InvertedIndex>(){
			public int compare(InvertedIndex l1, InvertedIndex l2)
			{
				return l1.getListSize() - l2.getListSize();
			}
		});

		comp.greater = true;
		// for (InvertedIndex i : list)
		// 	System.out.println(i.getListSize());
		int did = 0;
		int d=-1;

		// run until we have exaughsted all the documents of the shorter list
		while (did != -1)
		{
			// get the document from the shortest list
			did = list.get(0).nextGEQ(did);

			// check if other documents have the same document ID
			for (int i = 0; ( i<list.size() && (d = list.get(i).nextGEQ(did)) == did ); ++i );

			// this is not a intersection, so assign the new did and start over
			if (d>did && did != -1) 
				did = d;
			// this is a intersection
			// compute the BM25 score
			else if (did != -1)
			{
				// compute the bm25 score for this document and push it onto the heap

				int N = urlTable.size(); // total number of documents in the collection
				// parameters for the bm25 function
				double k =  1.2;
				double b = .75;

				double bm25 = 0;

				// loop through each occurance of the list since the bm25 score will be the sum of all of them
				for (int i = 0; i < list.size(); ++i)
				{
					// this will return the 
					// get all the variables for the formula
					int nqi = list.get(i).getListSize(); // total number of documents that contain this term
					int ft = list.get(i).getFreq(did); // get the frequency for this term
					double avgDocumentLength = totalDocumentLength / (double) N;
					double documentLength =  urlTable.get(did).length;

					//System.out.println((N-ft+.5)/(ft+.5));

					// compute and sum the total bm25 score
					double secondTerm = k*((1-b) +b* (documentLength/avgDocumentLength));
					
					double idf = (double) (Math.log(Math.abs((N-ft+.5))/(ft+.5)) / Math.log(2)) * ((k+1)*ft)/(secondTerm + ft); // take the log of this value in base 2
					//System.out.println( Math.log((N-ft+.5)/(ft+.5))+ " * " + ((k+1)*ft)/(secondTerm + ft) );
					bm25 += idf;
						// double K = k1 * ((1-b) + b *(docLength/dAvg));
						// bm25Score += (Math.log((N-ft+.5)/(ft+.5)) / Math.log(2)) * ((k1+1)*ft)/(K + ft);
				}

				// add the user rank score
				// almost same as BM25
				UserRank tmp = userBasedRankTable.get(query);
				if (tmp != null)
				{
					int click = tmp.getFreq(did); // get click
					// only add score if there is a click
					if (click>0)
					{
						//double avgClicks = (double)click/(double)documentClickFreq; // #clicks for this doc/total number of clicks
						double secondTerm = k*((1-b) +b* (double)click/(double)documentClickFreq);
						// double firstTerm = (double)Math.log((documentClickFreq-click+.5)/(click+.5))/Math.log(2);	
						// System.out.println("====================== did: " + did);
						// System.out.println("Document clicks: " + click + " Total Document Clicks: " + documentClickFreq+  " Avg clicks: "+ avgClicks);
						// System.out.println("Document count: "+documentClickCount);
						// System.out.println("Second Term: " + secondTerm + " First Term: " + firstTerm + " first*second = " + secondTerm*firstTerm);

						bm25 += secondTerm;
					}
				}

				heap.add(new Pair<Integer,Double>(did,bm25));
				did++;// incremenet the DID
			}

		}
	}

/*---------------------------------------------------------------------------------------------------------- Classes */

	// holds the inverted index 
	class InvertedIndex{
	
		private ArrayList<InvertedList> list;
		private int lastId; // this is the saved state for the last Document ID

		// contructor for constructing an instance of the class
		public InvertedIndex()
		{
		
			list = new ArrayList<InvertedList>();
			lastId = 0;
		}

		public void addDocument(int doc, int freq, Date date)
		{
			// create a new document and add it to the list
			list.add(new InvertedList(doc,freq,date));
		}

		public int getListSize(){ return list.size();}

		// this method is used with DAAT
		// returns the next document greater than or equal to docID, returns -1 otherwise
		public int nextGEQ(int docId)
		{
			// if the passed in docId isnt valid, return -1
			if (docId == -1)
				return -1;

			for (int i = lastId; i < list.size();++i)
			{
				// if we have found a document ID atleast the value of docID, return it
				if (list.get(i).docID >= docId)
				{
					// save the state
					lastId = i;
					return list.get(i).docID;
				}
					
			}

			// we havent found a document
			return -1;
		}

		// this basically resets the saved value of lastId to 0
		public void resetLastId(){lastId = 0;}

		// get the frequency of the given document ID
		public int getFreq(int docId)
		{
			for (InvertedList l : list)
			{
				if (l.docID == docId)
					return l.freq;
			}
			// else return - 1
			return -1;
		}

		// get the year of the given document ID
		public Date getDate(int docId)
		{
			for (InvertedList l : list)
			{
				if (l.docID == docId)
					return l.date;
			}
			// else return - 1
			return null;
		}



		public String toString()
		{
			StringBuilder builder = new StringBuilder();

			// loop through each element of the array and output it
			for(InvertedList l : list )
				builder.append(l.toString());

			return builder.toString();
		}

			// classs to hold the inverted list
		class InvertedList{

			private	int docID;
			private	int freq;
			//private	int year;
			private Date date;

			public InvertedList(int docId,int freq,Date date)
			{
				this.docID = docId;
				this.freq = freq;
				this.date = date;
			}
			public String toString()
			{
				return  ":" + docID + ":" + freq + ":" + date;
			}		
		}

	}

	class DataComparator implements Comparator<Pair<Integer,Double>>
	{
		public boolean greater = true; // this will tell us how to sort the values

		@Override
		public int compare(Pair<Integer,Double> p1, Pair<Integer,Double> p2)
		{
			//return p1.getSecond().compareTo(p2.getSecond());
			Double x1 = p1.second;
			Double x2 =  p2.second;

			// depending on the query time, we want to sort it in ascending order or descending order
			if (greater)
				return x2.compareTo(x1);
			else
			{
				// reset the value back to greater than since thats what most of out queries are
				//greater = true;
				return x1.compareTo(x2);
			}
				
		}
	}

	class Document{
		public String docUrl;
		public int length;
		public Date date;

		public Document(String docUrl,Date date, Integer length)
		{
			this.docUrl = docUrl;
			this.length = length;
			this.date = date;
			//System.out.println(length);
		}
	}

	// used to store the user Ranked documents
	class UserRank{
		ArrayList<Pair<Integer,Integer>> rankList;

		public UserRank()
		{
			rankList = new ArrayList<Pair<Integer,Integer>>();
		}
		// inserts the rank for this page into the list if it doesnt exist yet
		// otherwise, we find it the rank and increment it's value
		public void addRank(int docid)
		{
			Integer d = new Integer(docid);
			for (Pair<Integer,Integer> p: rankList)
			{
				//System.out.println(p.first + "="+docid);
				if (p.first.equals(docid))
				{
					p.second = p.second+1;
					return;
				}			
			}
			// otherwise insert it
			//System.out.println("INSERTzi");
			rankList.add(new Pair<Integer,Integer>(docid,1)); // set the freq to one
		}
		public void addRank(int docid, int freq)
		{
			rankList.add(new Pair<Integer,Integer>(docid,freq));
		}

		public int getFreq(int docid)
		{
			Integer d = new Integer(docid);
			for (Pair<Integer,Integer> p: rankList)
			{
				if (p.first.equals(d))			
					return p.second;
			}

			return -1;
		}

		public String toString()
		{
			//StringBuilder builder = StringBuilder();
			StringBuilder builder = new StringBuilder();
			for (Pair<Integer,Integer> p: rankList)
			{
				builder.append(":"+p.first + ":" + p.second);
				//System.out.println(builder.toString());
			}

			return builder.toString();
		}

	}

	// pair class to hold the document information, that require two elements
	class Pair<T1,T2>{
		public T1 first;
		public T2 second;

		public Pair(T1 f, T2 s)
		{
			this.first = f;
			this.second = s;
		}
	}
}