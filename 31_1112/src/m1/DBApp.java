package m1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

class DBAppException extends Exception {

}

class DBEngineException extends Exception {

}

public class DBApp implements Serializable {

	ArrayList<String> tableNames = new ArrayList<String>();
	static ArrayList<Integer> rec = null; // dah bta3 el selector thingy xD el
											// byedeeny el arkam xD

	// int pointer = 0;
	// int n = 200;
	int n; // DBApp("n", "200");
	// System.out.println(n);

	// boolean isEmp = false;
	// n 3ayza agebha mn el prop file thingy
	ArrayList<ArrayList<ArrayList<String>>> kolo = new ArrayList<ArrayList<ArrayList<String>>>();
	ArrayList<ArrayList<String>> btoo3y = new ArrayList<ArrayList<String>>();
	ArrayList<String> types = new ArrayList<String>();
	ArrayList<String> colnamee = new ArrayList<String>();
	ArrayList<String> refref = new ArrayList<String>();
	ArrayList<String> refwhat = new ArrayList<String>();
	ArrayList<String> csvFile = new ArrayList<String>();
	static FileWriter fw; // = new FileWriter("/path.txt");
	// System.out.println("");

	public static String configReader(String whatIwantToRead) {
		// try {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream(
					"C:/Users/Mena/Desktop/sem 6/db/proj/31_1112/config/DBApp.properties");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			prop.load(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String s = prop.getProperty(whatIwantToRead);
		return s;

	}

	public void configerWriter(String whatIwanttowrite, String itsValue) {

		Properties prop = new Properties();
		OutputStream output = null;

		try {
			output = new FileOutputStream(
					"C:/Users/Mena/Desktop/sem 6/db/proj/31_1112/config/DBApp.properties");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// set the properties value
		prop.setProperty(whatIwanttowrite, itsValue);
		// save properties to project root folder
		try {
			prop.store(output, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void init() throws IOException {
		n = Integer.parseInt(configReader("MaximumRowsCountinPage"));
		// System.out.println(n);
		// configerWriter("n", "200");
	}

	public void csvWrite(String strTableName, String colName, String colType,
			String string, boolean key,  boolean indexed) throws IOException {
		fw.append(strTableName + "," + colName + "," + colType + "," + key
				+ "," + indexed + "," + string + "\n");
		fw.flush();
	}

	public void createIndex(String strTableName, String strColName)
			throws DBAppException, FileNotFoundException, IOException, ClassNotFoundException {
		
		File p = new File("C:/Users/Mena/Desktop/sem 6/db/proj/31_1112");
		File[] files = p.listFiles();
		boolean here = false;
		boolean primHere = false;
		for (int i = 0; i < files.length; i++) {
			if (files[i].isFile()) {
				String fullName = files[i].getName();
				if (fullName.length() >= 5) {
					String extension = fullName.substring(
							fullName.length() - 5, fullName.length());
					if (extension.equals("class")) {
						String fileName = fullName.substring(0,
								fullName.length() - 6);
						String[] split = fileName.split("-");
						if (split[0].equals(strTableName)){
								here = true;
						}
						if (split[1].equals(strColName)){
							primHere = true;
						}
						}
					}
			}
		}
		if (here == false){
			//law awel marra a3melaha btree 
			tableChecker(strTableName);
		}
//			BTree  c = new BTree<Comparable<TKey>, int>();
//			//3ayza at2aked mn el btree dy 
//
//			ObjectOutputStream lala1 = new ObjectOutputStream(new FileOutputStream(
//					new File(strTableName + "-" + strColName + ".class")));
//			lala1.writeObject(lala1);
//			lala1.close();
//			lala1.flush();
//			
//			return; 
		else {
			if (primHere == false ){
				BTree c = new BTree();
				ObjectOutputStream lala1 = new ObjectOutputStream(new FileOutputStream(
						new File(strTableName + "-" + strColName + ".class")));
				lala1.writeObject(c);
				lala1.close();
				lala1.flush();
			
				int pageNumber = 0;
				for (int i = 0; i < files.length; i++) {
					if (files[i].isFile()) {
						String fullName = files[i].getName();
						if (fullName.length() >= 5) {
							String extension = fullName.substring(
									fullName.length() - 5, fullName.length());
							if (extension.equals("class")) {
								String fileName = fullName.substring(0,
										fullName.length() - 6);
								String[] split = fileName.split(",");
								int curPage = Integer.parseInt(split[1]);

								if (split[0].equals(strTableName))
									pageNumber = Math.max(curPage, pageNumber);
							}
						}
					}
				}
				
				for (int a =1; a<pageNumber +1; a++){
					ObjectInputStream ois = new ObjectInputStream(new FileInputStream(
							new File(strTableName + "," + a + ".class")));
					Page page = (Page) ois.readObject();
					ois.close();
					page.pageParseToTree(strColName, a,c);
					}
				
				
				
				//get all the pages, then ha-insert bas el values el 3al 
				
				
				
			//ha parse 3al table kolo w adakhal elvalues 
				//gonna loop 3ala kol el pages w kol record, w agy 3and kol record a3mel concat 3ala rakam el page and index i of rec
				//then add that to the node el ha3melo insertion 
			}
		}
		}
		
	

	public void csvRead() throws IOException {

		// InputStream input = (InputStream) getClass().getClassLoader()
		// .getResourceAsStream("MetaData.csv");
		BufferedReader bf = new BufferedReader(
				new FileReader(
						"C:\\Users\\Mena\\Desktop\\sem 6\\db\\proj\\31_1112\\data\\MetaData.csv"));
		String line = bf.readLine();
		while (line != null) {
			// System.out.println(line);
			csvFile.add(line);
			line = bf.readLine();
		}

	}

	public boolean tableChecker(String strTableName) {

		File p = new File("C:/Users/Mena/Desktop/sem 6/db/proj/31_1112");
		File[] files = p.listFiles();
		for (int i = 0; i < files.length; i++) {
			if (files[i].isFile()) {
				String fullName = files[i].getName();
				if (fullName.length() >= 5) {
					String extension = fullName.substring(
							fullName.length() - 5, fullName.length());
					if (extension.equals("class")) {
						String fileName = fullName.substring(0,
								fullName.length() - 6);
						String[] split = fileName.split(",");

						if (split[0].equals(strTableName))
							return true;
					}
				}
			}
		}
		return false;
	}

	public void createTable(String strTableName,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameRefs, String strKeyColName)
			throws Exception {

		htblColNameType.put("TouchDate", "Date");
		htblColNameType.put("Deleted", "String");
		// el bta3a el karim 2al 3aleeha...bta3et el serializable (nes2al el
		// TA).
		// 3ayza azabat 7war el size bta3 tany hashtable 3shan howa msh nafs
		// size el awalany
		boolean isEmp = false;

		// array that checks if i had this table name before or not
		boolean exists = tableChecker(strTableName);
		if (exists == true) {
			throw new Exception("tableAlreadyHere");

		}
		// tableNames.add(strTableName);
		// System.out.println(htblColNameRefs.size());
		Iterator<String> iter = htblColNameType.keySet().iterator();

		colnamee = new ArrayList();
		types = new ArrayList();
		refwhat = new ArrayList();
		refref = new ArrayList();
		
		while (iter.hasNext()) {
			String temp = iter.next();
			colnamee.add(temp);
			types.add(htblColNameType.get(temp));
		}
		if (htblColNameRefs.isEmpty()) {
			isEmp = true;
		} else {
			Iterator<String> it = htblColNameRefs.keySet().iterator();
			// int s =0;
			while (it.hasNext()) {
				String temp = it.next();
				refwhat.add(temp);
				// if (temp == null)
				// temp = " ";
				refref.add(htblColNameRefs.get(temp));
			}
		}
		// refref = (ArrayList<String>) htblColNameRefs.values();

		btoo3y.add(types);
		btoo3y.add(colnamee);
		btoo3y.add(refref);
		btoo3y.add(refwhat);
		kolo.add(btoo3y);
		// el talata arrays dool fe arraylist
		// el primary key howa el get sent msh kol marra..gotta check 3aleeh
		int indexy = -1;
		int z = types.size();
		// System.out.println(z);
		int a;
		if (isEmp == true) {
			for (a = 0; a < z; a++) {
				if (strKeyColName.equals(colnamee.get(a)))
					csvWrite(strTableName, colnamee.get(a), types.get(a), null,
							 true, false);

				else
					csvWrite(strTableName, colnamee.get(a), types.get(a), null, false,
					false);
				// can it write null fel file?
			}
		} else {
			for (a = 0; a < z; a++) {
				for (int c = 0; c < refref.size(); c++) {
					indexy = -1;
					// 3ayza ageeb el part el abl el dot 3shan a-compare it with
					// the other stuff
					// facutly_id, tablename.ID

					// System.out.println(refwhat.get(c) + "get 0 of refwhat" );
					// System.out.println(ar[0] + "get 0 " + ar[1] + "get 1" );

					if (colnamee.get(a).equals(refwhat.get(c))) {
						indexy = c;
						break;
					}
				}

				if (indexy == -1) { // ya3ny msh mawgood
					if (strKeyColName.equals(colnamee.get(a)))
						csvWrite(strTableName, colnamee.get(a), types.get(a),
								null, true, false);
					else
						csvWrite(strTableName, colnamee.get(a), types.get(a),
								null, false, false);

				} else {

					// String [] ar = refref.get(indexy).split(".");
					// malooo? xD
					if (strKeyColName.equals(colnamee.get(a)))

						csvWrite(strTableName, colnamee.get(a), types.get(a),
								refref.get(indexy), true, false);
					else
						csvWrite(strTableName, colnamee.get(a), types.get(a),
								refref.get(indexy), false, false);

				}
			}
			// System.out.println(colnamee.size() + " " + types.size() + " "
			// + refref.size());
			// System.out.println("arraylists of arra of arr" + kolo.size());
			// System.out.println("tables " + tableNames.size()) ;
		}

		Page c = new Page(strTableName, n);
		// myPages.add(c);
		// mainPagesArray.add(myPages);

		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(
				new File(strTableName + ",1.class")));
		oos.writeObject(c);
		oos.close();
		oos.flush();
		// Employee, Name, String, department.name,
		/*
		 * n3mel array fy kol el table names 3shan el exceptions prmiary key by
		 * deafult indexed
		 */
		
//		createIndex(strTableName, strKeyColName);
		BTree cc = new BTree<>();
		
		ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(
				new File(strTableName + "-" +strKeyColName + ".class")));
		o.writeObject(cc);
		o.close();
		oos.flush();
	}

	public void insert(String tableName, Hashtable<String, Object> hashTable)
			throws ClassNotFoundException, IOException {

		//BTREE INSERT LAZEM ADEEHA EL STRING BTA3 EL CONCAT, W DY HA3MELHA LAMMA AGY ADAKHAL NEW REC OR LAW HA-LOOP 3ALEEH
		
		File p = new File("C:/Users/Mena/Desktop/sem 6/db/proj/31_1112");
		File[] files = p.listFiles();
		int pageNumber = 0;
		ArrayList<String> trees3andy = new ArrayList<String>();
		for (int i = 0; i < files.length; i++) {
			if (files[i].isFile()) {
				String fullName = files[i].getName();
				if (fullName.length() >= 5) {
					String extension = fullName.substring(
							fullName.length() - 5, fullName.length());
					if (extension.equals("class")) {
						String fileName = fullName.substring(0,
								fullName.length() - 6);
						String[] split = fileName.split(",");
						String[] bTreeSplit = fileName.split("-");
						int curPage = Integer.parseInt(split[1]);

						
						//law fy eroor yeb2a e3mel if split not null
						
						if (split[0].equals(tableName))
							pageNumber = Math.max(curPage, pageNumber);
						if(bTreeSplit[0].equals(tableName))
							//if hashtable has that coloumn
							//law mawgood aslan 
							trees3andy.add(bTreeSplit[1]);
						//law fy errors shoof enny a3mel el arraylist be null fel awel 
					}
				}
			}
		}

		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(
				new File(tableName + "," + pageNumber + ".class")));
		Page page = (Page) ois.readObject();
		ois.close();

		if (page.counter == page.n) {
			Page p1 = new Page(tableName, n);
			p1.addRecord(hashTable);
			ObjectOutputStream oos = new ObjectOutputStream(
					new FileOutputStream(new File(tableName + ","
							+ (pageNumber + 1) + ".class")));
			oos.writeObject(p1);
			oos.close();
		
			while (!(trees3andy.isEmpty())){
				String value = trees3andy.get(0);
				trees3andy.remove(0);
				
				ObjectInputStream o = new ObjectInputStream(new FileInputStream(
						new File(tableName + "-" + value + ".class")));
				BTree tree = (BTree) o.readObject();
				o.close();
				int n = pageNumber + 1;
				String concat = n + "," + 0;
				tree.insert((Comparable) hashTable.get(value), concat);
			}
		
		} else {
			page.addRecord(hashTable);
			ObjectOutputStream oos = new ObjectOutputStream(
					new FileOutputStream(new File(tableName + "," + pageNumber
							+ ".class")));
			oos.writeObject(page);
			oos.close();
		

			while (!(trees3andy.isEmpty())){
				String value = trees3andy.get(0);
				trees3andy.remove(0);
				
				ObjectInputStream o = new ObjectInputStream(new FileInputStream(
						new File(tableName + "-" + value + ".class")));
				BTree tree = (BTree) o.readObject();
				o.close();
				int n = page.counter;
				String concat = pageNumber + "," + n;
				tree.insert((Comparable) hashTable.get(value), concat);
			}
		
		
		}
		
		
	}

	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws Exception {

//		if (selectFromTable(strTableName, htblColNameValue, "AND").hasNext()) {
//			throw new Exception("duplicate data");
//		} else {

			boolean exists = tableChecker(strTableName);

			if (exists) {
				htblColNameValue.put("Deleted", "false");
				insert(strTableName, htblColNameValue);

			} else
				throw new Exception("tableDoesNotExist");

			/*
			 * shoof el page law feeha makan load it w 7lt law mafeesh create
			 * new Page law aslan el table mawgood or not page getter w csv
			 * writer w reader }
			 */
	//	}
	}
	 

	public void updateTable(String strTableName, String strKey,
			Hashtable<String, Object> htblColNameValue) throws Exception {
		String myPrime = "";
		boolean la2eto = false;
		csvRead();
		
		ArrayList<String> allMyBs = new ArrayList<String>();
		for (int y = 0; y < csvFile.size(); y++) {
			// System.out.println("ANA GOWA EL FOR MWAHAHAH");
			String ar[] = csvFile.get(y).split(",");
			if (ar[0].equals(strTableName)) {
				if (ar[3].equals("true")) {
					myPrime = ar[1];
					break;
				}
			}
		}
		File p = new File("C:/Users/Mena/Desktop/sem 6/db/proj/31_1112");
		File[] files = p.listFiles();
		String position ="0"; 
		Hashtable<String, Object> oldVals = null;

		for (int i = 0; i < files.length; i++) {

			
			if (files[i].isFile()) {
				String fullName = files[i].getName();
				if (fullName.length() >= 5) {
					String extension = fullName.substring(
							fullName.length() - 5, fullName.length());
					if (extension.equals("class")) {
						String fileName = fullName.substring(0,
								fullName.length() - 6);
						String[] split = fileName.split(",");
						String[] treeSplit = fileName.split("-");

						
						if (treeSplit[0].equals(strTableName)){
							allMyBs.add(treeSplit[1]);
							continue;
							}
					
						//else, y3mel el normal update
						//MSH 3AYZA EL OLD CODE 3SHAN KEDA KEDA FL B+ 3ANDY INDEX 3AL PRIMARY KEY
//						if (split[0].equals(strTableName)){
//							
//							ObjectInputStream ois = new ObjectInputStream(
//									new FileInputStream(new File(strTableName
//											+ "," + split[1] + ".class")));
//							Page page = (Page) ois.readObject();
//							ois.close();
//							
//							page.updater(htblColNameValue, myPrime, strKey, page, split[1], strTableName);
//						}

					}
				}
			}
		}
		
		
		if(!(allMyBs.isEmpty())){
			String s = allMyBs.get(0);
			allMyBs.remove(s);
			//primary key keda! wohoo 
			
			ObjectInputStream ois = new ObjectInputStream(
					new FileInputStream(new File(strTableName
							+ "-" + s + ".class")));
			BTree tree = (BTree) ois.readObject();
			ois.close();
			
			 position = (String) tree.search((Comparable) htblColNameValue.get(strKey));
			//this should print el string el howa 7aga,row
			//need to update this row be values gdeeda yeb2a ha3mel eh, 
			String[] posSplit = position.split(",");
			
			ObjectInputStream ois2 = new ObjectInputStream(
					new FileInputStream(new File(strTableName
							+ "," + posSplit[0] + ".class")));
			Page page = (Page) ois2.readObject();
			ois2.close();
			oldVals = page.pageContent[Integer.parseInt(posSplit[1])];
			page.treeUpdater(oldVals, htblColNameValue);
		}	

		while(!allMyBs.isEmpty()){
			String s = allMyBs.get(0);
			ObjectInputStream ois = new ObjectInputStream(
					new FileInputStream(new File(strTableName
							+ "-" + s + ".class")));
			BTree tree = (BTree) ois.readObject();
			ois.close();
			Object w = oldVals.get(s);
//			hash
//			3ady el b-tree 3ayza a-search w a-get el position ad el position, update-o 

			String pointer = (String) tree.search((Comparable) w);
			if (pointer.equals(position)){
				tree.delete(pointer);
				tree.insert((Comparable) htblColNameValue.get(s), pointer);
			}
			
		}
	}

			//updated rec, need to update trees now, ALL OF EM 
			//only got string from first tree AND updated balue in original table. Need to update the rest of the 
			//b trees 
	
	//		ObjectInputStream ois = new ObjectInputStream(
//				new FileInputStream(new File(strTableName
//						+ "," + "1" + ".class")));
//        System.out.println( "pageBBB " +  ((Page) ois.readObject()).pageContent[50]);

//}	
//}

	public void deleteFromTable(String strTableName,
			Hashtable<String, Object> htblColNameValue, String strOperator)
			throws Exception {
		
		/* assume that size of hstbl 1, NO OPERATOR  
		 * khod col w dawar 3ala b+ bta3ty, search w get num and rec place 
		 * put in arraylist and loop 3al b + w gowa el b+ haloop
		 * 
		 */
		ArrayList<Hashtable<String, Object>> recs = new ArrayList<Hashtable<String,Object>>();
		String address = "";
		File p = new File("C:/Users/Mena/Desktop/sem 6/db/proj/31_1112");
		File[] files = p.listFiles();
		ArrayList<String> elTrees = new ArrayList<String>();

		for (int i = 0; i < files.length; i++) {
//			ArrayList<String> elTrees = new ArrayList<String>();
			if (files[i].isFile()) {
				String fullName = files[i].getName();
				if (fullName.length() >= 5) {
					String extension = fullName.substring(
							fullName.length() - 5, fullName.length());
					if (extension.equals("class")) {
						String fileName = fullName.substring(0,
								fullName.length() - 6);
						String[] split = fileName.split(",");
						String[] splitTree = fileName.split("-");
						
						if (htblColNameValue.size() ==1){		
						
						if (splitTree[0].equals(strTableName)){
							elTrees.add(splitTree[1]);
							continue;
						}
						}
						else {
						//normal delete if i have an operator ya3ny 
							
//						if (elTrees.isEmpty()){
						if (split[0].equals(strTableName)) {
							ObjectInputStream ois = new ObjectInputStream(
									new FileInputStream(new File(strTableName
											+ "," + split[1] + ".class")));
							Page page = (Page) ois.readObject();
							ois.close();
							page.delSearch(htblColNameValue, strOperator, page, split[1],strTableName );
					
						}
						}
						}
					
				
			}}
		}
		//esm el b+ trees el 3andy..ha-delete mn kol el b+ trees + mn el 3ady table 
		if(!(elTrees.isEmpty())){
			String s = elTrees.get(0);
			elTrees.remove(s);
			//dah bta3 el prim key btw 
			ObjectInputStream ois = new ObjectInputStream(
					new FileInputStream(new File(strTableName
							+ "-" + s + ".class")));
			BTree tree = (BTree) ois.readObject();
			ois.close();
			address = (String) tree.search((Comparable) htblColNameValue.get(s));
			String spliter [] = address.split(",");

			ObjectInputStream os = new ObjectInputStream(
					new FileInputStream(new File(strTableName
							+ "," + spliter[0] + ".class")));
			Page page = (Page) os.readObject();
			os.close();

			recs.add(page.pageContent[Integer.parseInt(spliter[1])]);
			tree.delete((Comparable) htblColNameValue.get(s));
			
		}
		while(!(elTrees.isEmpty())){
			String s = elTrees.get(0);
			elTrees.remove(s);
			ObjectInputStream ois = new ObjectInputStream(
					new FileInputStream(new File(strTableName
							+ "-" + s + ".class")));
			BTree tree = (BTree) ois.readObject();
			ois.close();
			Object c = recs.get(0).get(s); //asume en fy record wa7ed bas 
			tree.delete((Comparable) c);
	
		}
	}

	public Iterator selectFromTable(String strTable,
			Hashtable<String, Object> htblColNameValue, String strOperator)
			throws Exception {
		// return null;
		// System.out.println("meh " + strTable + " " + strOperator );
		 
		//assume while searching using B+ tree, hashtable has one row only. 
		//ex select from bla bla where age = 13 
		
		
		File p = new File("C:/Users/Mena/Desktop/sem 6/db/proj/31_1112");
		File[] files = p.listFiles();
	//	System.out.println("len " + files.length);
		// int pageNumber = 1;
		Queue<String> results = new LinkedList<String>();
		for (int i = 0; i < files.length; i++) {
			//System.out.println(i + " " + files.length);
			if (files[i].isFile()) {
				String fullName = files[i].getName();
				if (fullName.length() >= 5) {
					String extension = fullName.substring(
							fullName.length() - 5, fullName.length());
					if (extension.equals("class")) {
						String fileName = fullName.substring(0,
								fullName.length() - 6);
						String [] splitBTree = fileName.split("-");
						String[] split = fileName.split(",");
						
						// System.out.println(fullName);
						// System.out.println(split[0]);
						Iterator<String> iter3alhtbl = htblColNameValue.keySet().iterator();

						while (iter3alhtbl.hasNext()) {
							String nameOfItsCol = iter3alhtbl.next();
						if (splitBTree[0].equals(strTable)){
							if (splitBTree[1].equals(nameOfItsCol)){
							
						ObjectInputStream o = new ObjectInputStream(
								new FileInputStream(new File(strTable + "-"
										+ splitBTree[1] + ".class")));

						BTree whatever = (BTree) o.readObject();
						o.close();
								
					String returnedAddress =(String) whatever.search((Comparable) htblColNameValue.get(nameOfItsCol));
						
						//el mafroud law dy sa7 yeb2a yeraga3lena el address wl value aw 7aga keda 
						//yeb2a keda ma3aya makan el record, ha-get el value 3alatool 
						//load el page bta3to 
						
						//by convention en el string bta3 el address dah maslan haykoon pagenum, rec num
						//ex page 1,3
						// page =1 and row = 3
						
						String split3aladd [] = returnedAddress.split(",");
						
						
						ObjectInputStream ois = new ObjectInputStream(
								new FileInputStream(new File(strTable + ","
										+ split3aladd[0] + ".class")));
						Page page = (Page) ois.readObject();
						ois.close();
						Hashtable<String, Object> returnedHash = page.pageContent[Integer.parseInt(split3aladd[1])];
						Iterator<String> iter = returnedHash.keySet().iterator();

						while (iter.hasNext()) {
							String bla= iter.next();
							results.add((String) returnedHash.get(bla));
							}
						}
						
						else {
						
						if (split[0].equals(strTable)) {

							ObjectInputStream ois = new ObjectInputStream(
									new FileInputStream(new File(strTable + ","
											+ split[1] + ".class")));
							Page page = (Page) ois.readObject();
							ois.close();
							ArrayList<String> t = page.search(htblColNameValue,
									strOperator);

//							System.out
//									.println("weselt le 3and page search w kharagt");
							// System.out.println(t + "T");
							for (int j = 0; j < t.size(); j++) {
								results.add(t.get(j));
							}
							// System.out.println(results.get(results.size()-1));
							// hageeb esm el coloumn mn el page then ashoofo
							// equals el bta3a wala la2 law equal
						}
					}
					}
				}
			}
		}
		
		}	// System.out.println(results.size() + strTable);

		}
			for (int i = 0; i < results.size(); i++)
				System.out.println(results.remove());

			Iterator<String> it = new Iterator<String>() {

				@Override
				public String next() {
					return results.poll();
				}

				@Override
				public boolean hasNext() {
					return !results.isEmpty();
				}
			};

			return it;

		}


	public static void main(String[] args) throws Exception {
		// creat a new DBApp
		DBApp myDB = new DBApp();

		fw = new FileWriter("data/MetaData.csv");
		// initialize it
		myDB.init();

		// creating table "Faculty"

		Hashtable<String, String> fTblColNameType = new Hashtable<String, String>();
		fTblColNameType.put("ID", "Integer");
		fTblColNameType.put("Name", "String");

		Hashtable<String, String> fTblColNameRefs = new Hashtable<String, String>();

		myDB.createTable("Faculty", fTblColNameType, fTblColNameRefs, "ID");
		fw.flush();
		// creating table "Major"

		Hashtable<String, String> mTblColNameType = new Hashtable<String, String>();
		mTblColNameType.put("ID", "Integer");
		mTblColNameType.put("Name", "String");
		mTblColNameType.put("Faculty_ID", "Integer");

		Hashtable<String, String> mTblColNameRefs = new Hashtable<String, String>();
		mTblColNameRefs.put("Faculty_ID", "Faculty.ID");

		myDB.createTable("Major", mTblColNameType, mTblColNameRefs, "ID");

		// creating table "Course"

		Hashtable<String, String> coTblColNameType = new Hashtable<String, String>();
		coTblColNameType.put("ID", "Integer");
		coTblColNameType.put("Name", "String");
		coTblColNameType.put("Code", "String");
		coTblColNameType.put("Hours", "Integer");
		coTblColNameType.put("Semester", "Integer");
		coTblColNameType.put("Major_ID", "Integer");

		Hashtable<String, String> coTblColNameRefs = new Hashtable<String, String>();
		coTblColNameRefs.put("Major_ID", "Major.ID");

		myDB.createTable("Course", coTblColNameType, coTblColNameRefs, "ID");
		//
		// // creating table "Student"

		Hashtable<String, String> stTblColNameType = new Hashtable<String, String>();
		stTblColNameType.put("ID", "Integer");
		stTblColNameType.put("First_Name", "String");
		stTblColNameType.put("Last_Name", "String");
		stTblColNameType.put("GPA", "Double");
		stTblColNameType.put("Age", "Integer");

		Hashtable<String, String> stTblColNameRefs = new Hashtable<String, String>();

		myDB.createTable("Student", stTblColNameType, stTblColNameRefs, "ID");
		//
		// // // creating table "Student in Course"

		Hashtable<String, String> scTblColNameType = new Hashtable<String, String>();
		scTblColNameType.put("ID", "Integer");
		scTblColNameType.put("Student_ID", "Integer");
		scTblColNameType.put("Course_ID", "Integer");

		Hashtable<String, String> scTblColNameRefs = new Hashtable<String, String>();
		scTblColNameRefs.put("Student_ID", "Student.ID");
		scTblColNameRefs.put("Course_ID", "Course.ID");

		myDB.createTable("Student_in_Course", scTblColNameType, scTblColNameRefs, "ID");

		// insert in table "Faculty"

		Hashtable<String, Object> ftblColNameValue1 = new Hashtable<String, Object>();
		ftblColNameValue1.put("ID", Integer.valueOf("1"));
		ftblColNameValue1.put("Name", "Media Engineering and Technology");
		myDB.insertIntoTable("Faculty", ftblColNameValue1);

		Hashtable<String, Object> ftblColNameValue2 = new Hashtable<String, Object>();
		ftblColNameValue2.put("ID", Integer.valueOf("2"));
		ftblColNameValue2.put("Name", "Management Technology");
		myDB.insertIntoTable("Faculty", ftblColNameValue2);

		for (int i = 0; i < 300; i++) {
			Hashtable<String, Object> ftblColNameValueI = new Hashtable<String, Object>();
			ftblColNameValueI.put("ID", Integer.valueOf(("" + (i + 2))));
			ftblColNameValueI.put("Name", "f" + (i + 2));
			myDB.insertIntoTable("Faculty", ftblColNameValueI);
		}

		// insert in table "Major"

		Hashtable<String, Object> mtblColNameValue1 = new Hashtable<String, Object>();
		mtblColNameValue1.put("ID", Integer.valueOf("1"));
		mtblColNameValue1.put("Name", "Computer Science & Engineering");
		mtblColNameValue1.put("Faculty_ID", Integer.valueOf("1"));
		myDB.insertIntoTable("Major", mtblColNameValue1);

		Hashtable<String, Object> mtblColNameValue2 = new Hashtable<String, Object>();
		mtblColNameValue2.put("ID", Integer.valueOf("2"));
		mtblColNameValue2.put("Name", "Business Informatics");
		mtblColNameValue2.put("Faculty_ID", Integer.valueOf("2"));
		myDB.insertIntoTable("Major", mtblColNameValue2);

		for (int i = 0; i < 300; i++) {
			Hashtable<String, Object> mtblColNameValueI = new Hashtable<String, Object>();
			mtblColNameValueI.put("ID", Integer.valueOf(("" + (i + 2))));
			mtblColNameValueI.put("Name", "m" + (i + 2));
			mtblColNameValueI
					.put("Faculty_ID", Integer.valueOf(("" + (i + 2))));
			myDB.insertIntoTable("Major", mtblColNameValueI);
		}

		// insert in table "Course"

		Hashtable<String, Object> ctblColNameValue1 = new Hashtable<String, Object>();
		ctblColNameValue1.put("ID", Integer.valueOf("1"));
		ctblColNameValue1.put("Name", "Data Bases II");
		ctblColNameValue1.put("Code", "CSEN 604");
		ctblColNameValue1.put("Hours", Integer.valueOf("4"));
		ctblColNameValue1.put("Semester", Integer.valueOf("6"));
		ctblColNameValue1.put("Major_ID", Integer.valueOf("1"));
		myDB.insertIntoTable("Course", ctblColNameValue1);

		Hashtable<String, Object> ctblColNameValue2 = new Hashtable<String, Object>();
		ctblColNameValue2.put("ID", Integer.valueOf("1"));
		ctblColNameValue2.put("Name", "Data Bases II");
		ctblColNameValue2.put("Code", "CSEN 604");
		ctblColNameValue2.put("Hours", Integer.valueOf("4"));
		ctblColNameValue2.put("Semester", Integer.valueOf("6"));
		ctblColNameValue2.put("Major_ID", Integer.valueOf("2"));
		myDB.insertIntoTable("Course", ctblColNameValue2);

		for (int i = 0; i < 300; i++) {
			Hashtable<String, Object> ctblColNameValueI = new Hashtable<String, Object>();
			ctblColNameValueI.put("ID", Integer.valueOf(("" + (i + 2))));
			ctblColNameValueI.put("Name", "c" + (i + 2));
			ctblColNameValueI.put("Code", "co " + (i + 2));
			ctblColNameValueI.put("Hours", Integer.valueOf("4"));
			ctblColNameValueI.put("Semester", Integer.valueOf("6"));
			ctblColNameValueI.put("Major_ID", Integer.valueOf(("" + (i + 2))));
			myDB.insertIntoTable("Course", ctblColNameValueI);
		}

		// insert in table "Student"

		for (int i = 0; i < 500; i++) {
			Hashtable<String, Object> sttblColNameValueI = new Hashtable<String, Object>();
			sttblColNameValueI.put("ID", Integer.valueOf(("" + i)));
			sttblColNameValueI.put("First_Name", "FN" + i);
			sttblColNameValueI.put("Last_Name", "LN" + i);
			sttblColNameValueI.put("GPA", Double.valueOf("0.7"));
			sttblColNameValueI.put("Age", Integer.valueOf("20"));
			myDB.insertIntoTable("Student", sttblColNameValueI);
			// changed it to student instead of course
		}

		// selecting

		Hashtable<String, Object> changedStuff = new Hashtable<String, Object>();
		changedStuff.put("GPA", 3.3);
		changedStuff.put("Age", 37);

		Hashtable<String, Object> del = new Hashtable<String, Object>();
		del.put("GPA", 0.7);
		del.put("ID", 150);
		
		myDB.deleteFromTable("Student", del, "AND");

		myDB.updateTable( "Student", "50", changedStuff);

		Hashtable<String, Object> stblColNameValue4 = new Hashtable<String, Object>();
		stblColNameValue4.put("GPA", Double.valueOf("3.3"));
		stblColNameValue4.put("Age", Integer.valueOf("37"));
		//stblColNameValue4.put("ID", Integer.valueOf("50"));
		//System.out.println("dakhalt fel ba3d el updater");
		long startTime = System.currentTimeMillis();
		 Iterator myIt4 = myDB.selectFromTable("Student", stblColNameValue4, "AND");
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println(totalTime);
		 while (myIt4.hasNext()) {
		 System.out.println(myIt4.next() + "lala");
		 }

		System.out.println();

		Hashtable<String, Object> stblColNameValue = new Hashtable<String, Object>();
		//stblColNameValue.put("GPA", Double.valueOf("0.7"));
	//	stblColNameValue.put("Age", Integer.valueOf("20"));
		stblColNameValue.put("ID", Integer.valueOf("100"));
		//System.out.println("dakhalt fel ba3d el updater");
		long startTime1 = System.currentTimeMillis();
		Iterator myIt = myDB
				.selectFromTable("Student", stblColNameValue, "AND");
		long endTime1 = System.currentTimeMillis();
		long totalTime1 = endTime1 - startTime1;
		System.out.println(totalTime1);
		while (myIt.hasNext()) {
			System.out.println(myIt.next() + "lala2");
		}

		System.out.println();
		// feel free to add more tests
		Hashtable<String, Object> stblColNameValue3 = new Hashtable<String, Object>();
		stblColNameValue3.put("Name", "m7");
		stblColNameValue3.put("Faculty_ID", Integer.valueOf("7"));

		long startTime2 = System.currentTimeMillis();
		Iterator myIt2 = myDB
				.selectFromTable("Major", stblColNameValue3, "AND");
		long endTime2 = System.currentTimeMillis();
		long totalTime2 = endTime1 - startTime1;
		System.out.println(totalTime2);
		while (myIt2.hasNext()) {
			System.out.println(myIt2.next());
		}
	}
}
