package app;

import java.util.Scanner;

public class Util {

	@SuppressWarnings("resource")
	public static String inputRead(String msg) {
		Scanner scan = new Scanner(System.in);
		System.out.print(msg);
		String value = scan.nextLine();
		return value;
	}
}
