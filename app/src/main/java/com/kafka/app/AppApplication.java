package com.kafka.app;

import java.util.Date;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// @SpringBootApplication
public class AppApplication {

	public static void main(String[] args) {
		// SpringApplication.run(AppApplication.class, args);
		
		System.out.println(new Date(System.currentTimeMillis()));
		System.out.println(new Date(System.currentTimeMillis()+7200000));
	}	

}
