package app;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

public class Util {

	@SuppressWarnings("resource")
	public static String inputRead(String msg) {
		Scanner scan = new Scanner(System.in);
		System.out.print(msg);
		String value = scan.nextLine();
		return value;
	}
	
	public static <T extends Comparable<? super T>> List<T> asSortedList(Collection<T> c) {
	  List<T> list = new ArrayList<T>(c);
	  Collections.sort(list);
	  return list;
	}
}
