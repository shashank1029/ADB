package edu.nyu.adb;

public class Lock {
	
	/**
	 * There can be 2 types of locks on dataitems 1 is exclusive lock and the other shared lock
	 * Here, Read lock is a shared lock and write lock is exclusive lock
	 * Lock types
	 *
	 */
	public enum lockType{
		READ_LOCK,WRITE_LOCK;
	}
}
