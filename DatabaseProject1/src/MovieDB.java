/*****************************************************************************************
 * @file  MovieDB.java
 *
 * @author   John Miller
 */

import static java.lang.System.out;

/*****************************************************************************************
 * The MovieDB class makes a Movie Database.  It serves as a template for making other
 * databases.  See "Database Systems: The Complete Book", second edition, page 26 for more
 * information on the Movie Database schema.
 */
class MovieDB
{
    /*************************************************************************************
     * Main method for creating, populating and querying a Movie Database.
     * @param args  the command-line arguments
     */
    public static void main (String [] args)
    {
        out.println ();

        Table movie = new Table ("movie", "title year length genre studioName producerNo",
                                          "String Integer Integer String String Integer", "title year");

        Table cinema = new Table ("cinema", "title year length genre studioName producerNo",
                                            "String Integer Integer String String Integer", "title year");

        Table movieStar = new Table ("movieStar", "name address gender birthdate",
                                                  "String String Character String", "name");

        Table starsIn = new Table ("starsIn", "movieTitle movieYear starName",
                                              "String Integer String", "movieTitle movieYear starName");

        Table movieExec = new Table ("movieExec", "certNo name address fee",
                                                  "Integer String String Double", "certNo");

        Table studio = new Table ("studio", "name address presNo",
                                            "String String Integer", "name");

        Comparable [] film0 = { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
        Comparable [] film1 = { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
        Comparable [] film2 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
        Comparable [] film3 = { "Rambo", 1978, 100, "action", "Universal", 32355 };
        out.println ();
        movie.insert (film0);
        movie.insert (film1);
        movie.insert (film2);
        movie.insert (film3);
        movie.print ();

        Comparable [] film4 = { "Galaxy_Quest", 1999, 104, "comedy", "DreamWorks", 67890 };
        out.println ();
        cinema.insert (film2);
        cinema.insert (film3);
        cinema.insert (film4);
        cinema.print ();

        Comparable [] star0 = { "Carrie_Fisher", "Hollywood", 'F', "9/9/99" };
        Comparable [] star1 = { "Mark_Hamill", "Brentwood", 'M', "8/8/88" };
        Comparable [] star2 = { "Harrison_Ford", "Beverly_Hills", 'M', "7/7/77" };
        out.println ();
        movieStar.insert (star0);
        movieStar.insert (star1);
        movieStar.insert (star2);
        movieStar.print ();

        Comparable [] cast0 = { "Star_Wars", 1977, "Carrie_Fisher" };
        out.println ();
        starsIn.insert (cast0);
        starsIn.print ();

        Comparable [] exec0 = { 9999, "S_Spielberg", "Hollywood", 10000.00 };
        out.println ();
        movieExec.insert (exec0);
        movieExec.print ();

        Comparable [] studio0 = { "Fox", "Los_Angeles", 7777 };
        Comparable [] studio1 = { "Universal", "Universal_City", 8888 };
        Comparable [] studio2 = { "DreamWorks", "Universal_City", 9999 };
        out.println ();
        studio.insert (studio0);
        studio.insert (studio1);
        studio.insert (studio2);
        studio.print ();

        movie.save ();
        cinema.save ();
        movieStar.save ();
        starsIn.save ();
        movieExec.save ();
        studio.save ();

        movieStar.printIndex ();
        
        //--------------------- project

        out.println ();
        Table t_project = movie.project ("title year");
        t_project.print ();
        

        //--------------------- select

        out.println ();
        Table t_select = movie.select (t -> t[movie.col("title")].equals ("Star_Wars") &&
                                            t[movie.col("year")].equals (1977));
        t_select.print ();

        //--------------------- indexed select

        out.println ();
        Table t_iselect = movieStar.select (new KeyType ("Harrison_Ford"));
        t_iselect.print ();

        //--------------------- union

        out.println ();
        Table t_union = movie.union (cinema);
        t_union.print ();

        //--------------------- minus

        out.println ();
        Table t_minus = movie.minus (cinema);
        t_minus.print ();

        //--------------------- join

        out.println ();
        Table t_join = movie.join ("studioName", "name", studio);
        t_join.print ();

        out.println ();
        Table t_join2 = movie.join ("title year", "title year", cinema);
        t_join2.print ();

        out.println ();
		out.println ("All above is the test book. Below is for real.");
		
		// --------------------- For Real ---------------------
		// --------------------- Select

		out.println ();
		Table t_select_case1 = movie.select (new KeyType ("Star_Wars"));
		t_select_case1.print ();

		out.println ();
		Table t_select_case2 = movie.select (t -> t[movie.col ("title")].equals ("Rambo") && t[movie.col ("year")].equals (1978) || t[movie.col ("length")].equals (124));
		t_select_case2.print ();

		out.println ();
		Table t_select_case3 = movieStar.select (t -> t[movieStar.col ("name")].equals ("Nick"));
		t_select_case3.print ();

		// --------------------- Project

		out.println ();
		Table t_project_case1 = movie.project ("title year");
		t_project_case1.print ();

		out.println ();
		Table t_project_case2 = cinema.project ("title genre studioName");
		t_project_case2.print ();

		out.println ();
		Table t_project_case3 = studio.project ("abc presNo");
		t_project_case3.print ();
		
		// --------------------- Union
		
		out.println();
		Table t_union_case1 = movie.union (cinema);
		t_union_case1.print ();
		
		out.println ();
		Table t_union_case2 = movieStar.union (studio);
		t_union_case2.print ();
		
		out.println ();
		Table t_union_case3 = studio.union (starsIn);
		t_union_case3.print ();
		
		// --------------------- Minus
		
		out.println ();
		Table t_minus_case1 = movie.minus (cinema);
		t_minus_case1.print ();
		
		out.println ();
		Table t_minus_case2 = movieStar.minus (studio);
		t_minus_case2.print ();
		
		out.println ();
		Table t_minus_case3 = studio.minus (starsIn);
		t_minus_case3.print ();
		
		// --------------------- Join
		out.println ();
		Table t_join_case1 = movie.join ("studioName", "name", studio);
		t_join_case1.print ();
		
		out.println();
		Table t_join_case2 = movieStar.join ("name", "starName", starsIn);
		t_join_case2.print ();
    } // main

} // MovieDB class
