package somethingUtils.test;

import org.apache.commons.httpclient.HttpException;

import java.io.IOException;

public class test {
	public static void main(String[] args) throws HttpException, IOException {

		 
		String str = "local address = 192.168.4.213:44136 remote address = 192.168.4.212:57588";
       String[] Strip= str.split("remote"); 	
       String[] re=Strip[0].split("=");
       String[] lo=Strip[1].split("=");
       String[] xx=re[1].split(":");
		System.out.println("----"+xx[0]);
	    System.out.println("+++++"+lo[1]);
	
	}
		
	
}

