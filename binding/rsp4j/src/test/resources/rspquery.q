PREFIX : <http://streamreasoning.org/iminds/massif/>

SELECT *
FROM NAMED WINDOW  :win1 [RANGE 1 s, SLIDE 1s] ON STREAM :stream1
WHERE  {

    WINDOW ?w {
        ?s ?p ?o
    }

}
