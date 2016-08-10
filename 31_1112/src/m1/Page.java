package m1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.security.Timestamp;
import java.sql.Date;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;

public class Page implements Serializable {

	Hashtable<String, Object>[] pageContent;
	// the page is an array of objects
	String tableName;
	int n;
	int counter;

	public Page(String tableName, int n) {
		this.tableName = tableName;
		this.n = n;
		pageContent = new Hashtable[n];
		counter = 0;
	}
	
	public String convertToString(Hashtable<String, Object> ht) {
		Iterator<String> it = ht.keySet().iterator();
		String s = "";
		while (it.hasNext()) {
			String colName = it.next();
			Object colValue = ht.get(colName);

			if (colName.equals("Deleted"))
				continue;
			else
				s += colName + ": " + colValue + ", ";
		}
		return s;
	}

	// public ArrayList<String> search(Hashtable<String, Object> hashtable,
	// String operator) throws Exception {
	// ArrayList<String> valid = new ArrayList<String>();
	// System.out.println(valid + "ARRAYLIST VALID");
	// if (operator.equalsIgnoreCase("AND")) {
	// for (int i = 0; i < counter; i++) {
	// boolean flag = true;
	// Hashtable<String, Object> ht = pageContent[i];
	// Iterator<String> iter = hashtable.keySet().iterator();
	// // if (ht.get("Deleted").equals("true") ){
	// // continue;
	// // }
	// // else {
	// while (iter.hasNext()) {
	// String nameofcol = iter.next();
	// Object thisColValue = hashtable.get(nameofcol);
	// // System.out.println("dakhalt el iter bta3 el AND");
	// flag = flag
	// && thisColValue
	// .equals(ht.get(nameofcol));
	//
	// }
	// // }
	// if (flag)
	// valid.add(convertToString(pageContent[i]));
	// }
	// } else if (operator.equalsIgnoreCase("OR")) {
	// for (int i = 0; i < counter; i++) {
	// boolean flag = false;
	// Hashtable<String, Object> ht = pageContent[i];
	// Iterator<String> iter = hashtable.keySet().iterator();
	//
	// while (iter.hasNext()) {
	// String nameofcol = iter.next();
	// Object thiscolValue = ht.get(nameofcol);
	//
	// // if (nameofcol.equals("Deleted")
	// // && thiscolValue.equals("true")) {
	// // flag = false;
	// // }
	//
	// if (ht.get("Deleted").equals("true") ){
	// continue;
	// }
	// else {
	// flag = flag
	// || thiscolValue
	// .equals(hashtable.get(nameofcol));
	// }
	// }
	// if (flag)
	// valid.add(convertToString(ht));
	// }
	// } else
	// throw new Exception("Wrong operator");
	//
	// return valid;
	// }

	public ArrayList<String> search(Hashtable<String, Object> hashtable,
			String operator) throws Exception {
		ArrayList<String> valid = new ArrayList<String>();
		
		if (operator.equalsIgnoreCase("AND")) {
			for (int i = 0; i < counter; i++) {
				boolean flag = true;
				Hashtable<String, Object> pageht = pageContent[i];
				Iterator<String> iter = hashtable.keySet().iterator();

				while (iter.hasNext()) {
					String nameofcol = iter.next();
					Object thisColValue = pageht.get(nameofcol);
					// System.out.println("dakhalt el iter bta3 el AND");
					//System.out.println( thisColValue.equals(hashtable.get(nameofcol)));
					flag = flag
							&& thisColValue.equals(hashtable.get(nameofcol));
				}
			
				if (flag){
					//System.out.println(flag);
				//	System.out.println(pageContent[i]);
					valid.add(convertToString(pageContent[i]));
			}}
		} else if (operator.equalsIgnoreCase("OR")) {
			for (int i = 0; i < counter; i++) {
				boolean flag = false;
				Hashtable<String, Object> ht = pageContent[i];
				Iterator<String> iter = hashtable.keySet().iterator();

				while (iter.hasNext()) {
					String nameofcol = iter.next();
					Object thiscolValue = ht.get(nameofcol);

					flag = flag
							|| thiscolValue.equals(hashtable.get(nameofcol));
				}

				if (flag)
					valid.add(convertToString(ht));
			}
		} else
			throw new Exception("Wrong operator");

		return valid;
	}

	public void delSearch(Hashtable<String, Object> hashtable, String operator,
			 Page page, String pageNum,
				String strTableName) throws Exception {
		// ArrayList<String> valid = new ArrayList<String>();

		if (operator.equalsIgnoreCase("AND")) {
			for (int i = 0; i < counter; i++) {
				boolean flag = true;
				Hashtable<String, Object> ht = pageContent[i];
				Iterator<String> iter = hashtable.keySet().iterator();

				while (iter.hasNext()) {
					String nameofcol = iter.next();
					Object thisColValue = ht.get(nameofcol);
					// System.out.println("dakhalt el iter bta3 el AND");
					flag = flag
							&& thisColValue.equals(hashtable.get(nameofcol));
				}

				if (flag){
					// pageContent[i] = null;{
					ht.replace("Deleted", "true");
					ObjectOutputStream oos = new ObjectOutputStream(
							new FileOutputStream(new File(strTableName + "," + pageNum
									+ ".class")));
					oos.writeObject(page);
					oos.close();
					
					
					//System.out.println(ht.get("Deleted"));
//					System.out.println(ht + ",EKJAKLJLKA");
					}
			}
		} else if (operator.equalsIgnoreCase("OR")) {
			for (int i = 0; i < counter; i++) {
				boolean flag = false;
				Hashtable<String, Object> ht = pageContent[i];
				Iterator<String> iter = hashtable.keySet().iterator();

				while (iter.hasNext()) {
					String nameofcol = iter.next();
					Object thiscolValue = ht.get(nameofcol);

					flag = flag
							|| thiscolValue.equals(hashtable.get(nameofcol));
				}

				if (flag) {
					// pageContent[i]=null;
					ht.put("Deleted", "true");
					ObjectOutputStream oos = new ObjectOutputStream(
							new FileOutputStream(new File(strTableName + "," + pageNum
									+ ".class")));
					oos.writeObject(page);
					oos.close();
				}
			}
		} else
			throw new Exception("Wrong operator");

	}



	public String valueType(Object o) {
		if (o instanceof Integer)
			return "Integer";
		else if (o instanceof String)
			return "String";
		else if (o instanceof Double)
			return "Double";
		else if (o instanceof Boolean)
			return "Boolean";
		else if (o instanceof Date)
			return "Date";
		else
			return "";
	}

	public void addRecord(Hashtable<String, Object> htblColNameValue) {
		ZonedDateTime zdt = ZonedDateTime.now();

		// Date date = new Date();
		// SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");
		// String formattedDate = sdf.format(date);

		// java.util.Date date= new java.util.Date();
		// System.out.println();
		htblColNameValue.put("Date", zdt);

		pageContent[counter++] = htblColNameValue;
		
	}


	
	public void updater(Hashtable<String, Object> hashtable,
			String primKeyCol, String primElHaghayaro, Page page, String pageNum,
			String strTableName) throws FileNotFoundException, IOException {
//			int ret = -1;
			for (int i=0; i < counter; i++) {
				//Hashtable<String, Object> ht = pageContent[i];
				Iterator<String> iterOriginalHash = (pageContent[i]).keySet().iterator();
						if ((pageContent[i]).get(primKeyCol).toString().equals(primElHaghayaro)) {
							while (iterOriginalHash.hasNext()) {
						String nameofcol = iterOriginalHash.next();
						Object thisColValue = (pageContent[i]).get(nameofcol);
						// System.out.println("first while in updater" );
						// System.out.println("name of col" + nameofcol);
						// if (thisColValue.toString().equals(primKeyCol)){
						// loop 3aleeh shoof dah equals dah 7oto be hashtable.put w
						// edeelo el values bta3et key and value
						Iterator<String> iterHashIn = hashtable.keySet().iterator();

						while (iterHashIn.hasNext()) {
							String nameOfItsCol = iterHashIn.next();
							Object valueGdeed = hashtable.get(nameOfItsCol);
							// System.out.println("SARAAAAAA");
							// System.out.println(nameOfItsCol);
							// System.out.println(nameofcol);
							if (nameOfItsCol.equals(nameofcol)) {
								//System.out.println("?");
								(pageContent[i]).replace(nameOfItsCol, valueGdeed);
								//System.out.println("second while in updater");
								// System.out.println(ht);
								break;
							}
							// ba break barra el loop dy w bakamel fl loop el kbeera
							// 3shan i khalas added the value ya3ny
						}
					}
					ZonedDateTime zdt = ZonedDateTime.now();
					(pageContent[i]).replace("Date", zdt);
//					ret =i;
//					System.out.println(pageContent[i]);
					ObjectOutputStream oos = new ObjectOutputStream(
							new FileOutputStream(new File(strTableName + "," + pageNum
									+ ".class")));
					oos.writeObject(page);
					oos.close();
					//return pageContent[i];
				break;
						}
//			ArrayList<String> s =	search(pageContent[i], "AND");
//		System.out.println(pageContent[i]);
			}
//			return ret;
		}


	public void pageParseToTree(String colName, int pageNum, BTree c) {
		// TODO Auto-generated method stub
		//
		for (int i =0; i<counter; i++) {
			//hageeb el value of page[counter].get col 
			//theeeen ha concat page & pageNum 
			//then call insert fl tree c.blabla
			
			String pointer = pageNum + "," + i;
			Object key = pageContent[i].get(colName);
			c.insert((Comparable) key, pointer);
		//law fy error yeb2a ghayar method insert w khaleeha btakhod OBJEEEECT!! 
		
		}
		
	}

	public void treeUpdater(Hashtable<String, Object> pageContent2,
			Hashtable<String, Object> hashtable) {
		// TODO Auto-generated method stub
		Iterator<String> iterOriginalHash = pageContent2.keySet().iterator();
		while (iterOriginalHash.hasNext()) {
			String nameofcol = iterOriginalHash.next();
			Object thisColValue = (pageContent2).get(nameofcol);
			Iterator<String> newVals = hashtable.keySet().iterator();
			while (newVals.hasNext()) {
				String nameOfItsCol = newVals.next();
				Object valueGdeed = hashtable.get(nameOfItsCol);
				if (nameOfItsCol.equals(nameofcol)) {
					(pageContent2).replace(nameOfItsCol, valueGdeed);
					break;
				}
			}
		}
	}
	
	
}