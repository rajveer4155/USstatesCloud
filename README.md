# USstatesCloud
50 Wiki Web pages ' analysis dedicated to 50 US States using Hadoop Map-Reduce

### It performs following analysis:

1. Computes how many times the words “education”, “politics”, “sports”, and “agriculture” appear in each file. Then, the program outputs the number of states for which each of these words is dominant (i.e., appears more times than the other three words).

2. Identify all states that have the same ranking of these four words. For example, NY, NJ, PA may have the ranking 1. Politics; 2. Sports. 3. Agriculture; 4. Education (meaning “politics” appears more times than “sports” in the Wikipedia file of the state, “sports” appears more times than “agriculture”, etc.)

### Format of output file:

1. There is one output file.

2. All the outputs of 3 parts are in one file

3. The output file consists of 3 Sections - one for each part
 ************* OUTPUT: 1(A) - FREQUENCY OF EACH WORD IN DECREASING ORDER ***********:-	0
	< Section 1 goes here >
*********** OUTPUT: 1(B) - WORDWISE DOMINANT STATES COUNT ***********:-	0
	< Section 2 goes here >
*********** OUTPUT: PART 2-  STATES HAVING SAME RANKING OF ALL FOUR WORDS ; Each line represents different rank order ***************:-	0
	< Section 3 goes here >
  3.1 Section 3 output format:
      <States list with similar rank> ; Rank Order - <Rank Order of each word>
