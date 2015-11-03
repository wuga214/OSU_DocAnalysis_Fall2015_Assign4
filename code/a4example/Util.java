/*
 * Util class 
 *
 * Util class provides three static methods: parseTweet parses tweet record and return 
 * <userName, listOfMentionedUsers, listOfHashTags>; hasHashtag checks whether a tweet contains a hashtag; 
 * writeGraph writes edges to graph file. 
 *
 * Author: Liping Liu
 * 
*/

package a4example;

import scala.Tuple2;
import scala.Tuple3;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.util.HashSet;

import java.io.PrintWriter;

public class Util {

    // pattern of user name in the second line of the record
    private static final Pattern userPattern = Pattern.compile("\nU\thttp.*twitter.com/(\\w+)\n");
    // pattern of mentioned user
    private static final Pattern mentionPattern = Pattern.compile("@(\\w+)"); 
    // pattern of hashtag 
    private static final Pattern discussPattern = Pattern.compile("#(\\w+)"); 

    public static Tuple3<String, List<String>, List<String>  > parseTweet(String record) {
    

        // Parse the user name 
        String user = "?????";
        Matcher userMatcher = userPattern.matcher(record);
        if (userMatcher.find())
        {
            user = userMatcher.group(1); 
        }

        // Parse mentions users 
        Matcher mentionMatcher = mentionPattern.matcher(record);
        List<String> listOfMentionedUsers = new ArrayList<String>();
        while(mentionMatcher.find()) {
            listOfMentionedUsers.add(mentionMatcher.group(1));
        }

        // Parse hash-tags 
        Matcher discussMatcher = discussPattern.matcher(record);
        List<String> listOfHashTags = new ArrayList<String>();
        while(discussMatcher.find()) {
            listOfHashTags.add(discussMatcher.group(1));
        }

        // return the result in tuples with three elements  
        return new Tuple3<String, List<String>, List<String> > (user, listOfMentionedUsers, listOfHashTags);

    }
    
    //This function determines whether a tweet contains the hashtag 
    public static boolean hasHashtag(String record, String tag) {
    
          Pattern pattern = Pattern.compile("#" + tag + "[^\\w]"); 
          Matcher matcher = pattern.matcher(record);
          return matcher.find();
    }


    //This function writes edge list to csv file
    public static void writeGraph(List<Tuple2<String, Integer> > edgeList, String fileName) {
         
        try {
            PrintWriter writer = new PrintWriter(fileName);
            for (Tuple2<String, Integer> edge : edgeList)
            {
                //the tuple "edge" is collected from my MapReduce solution. 
                //Each edge is like this: <"user1,user2", 2>. The key value is already a string 
                //with two names concatenated   
                //Think about this question, how to avoid key-value pairs like this: <"user1,user2", 1> <"user2,user1", 1>?
                //If you do not do that, the reducer would not treat the two pairs as the same edge. 
                writer.println(edge._1() + "," + edge._2());
            }
            writer.close();
        } catch (Exception e)
        {
            System.err.println("Error in writing file " + fileName);
        }
    }
}

