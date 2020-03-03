import org.apache.spark

object Task4 {
        
    def setup () = {
        val graph_file = sc.textFile("../Data/yelp_top_users_friendship_graph.csv")
        graph_file.map(_.split(","))
    }

    def a () {
        val graph = setup()
        val nodes_by_edges = graph.map (edge => {
            val element_1 = if (edge(0)(0).toString == """"""") edge(0).slice(1, edge(0).length - 1) else edge(0)
            val element_2 = if (edge(1)(0).toString == """"""") edge(1).slice(1, edge(1).length - 1) else edge(1)
            (element_1, element_2)
        })
            .filter(tup => tup._1 != "src_user_id")
            .groupByKey
            .map (tup => {
                (tup._1, tup._2.toList.length)
            })
        
        println("The nodes with the highest number of connected edges: ")
        nodes_by_edges.sortBy(_._2, ascending=false).take(10).foreach(println)
    }

    def b () {
        val graph = setup()
        
        val nodes_with_out_edges = graph.map (edge => {
            val element_1 = if (edge(0)(0).toString == """"""") edge(0).slice(1, edge(0).length - 1) else edge(0)
            val element_2 = if (edge(1)(0).toString == """"""") edge(1).slice(1, edge(1).length - 1) else edge(1)
            (element_1, element_2)
        })
            .filter(_._1 != "src_user_id")
            .groupByKey
            .sortBy(_._2, ascending=false)
        
        val nodes_with_in_edges = graph.map (edge => {
            val element_1 = if (edge(0)(0).toString == """"""") edge(0).slice(1, edge(0).length - 1) else edge(0)
            val element_2 = if (edge(1)(0).toString == """"""") edge(1).slice(1, edge(1).length - 1) else edge(1)
            (element_2, element_1)
        })
            .filter(_._2 != "src_user_id")
            .groupByKey
            .sortBy(_._2, ascending=false)

        val sum_of_out_edges = nodes_with_out_edges
            .map (_._2.toList.length)
            .sum

        val sum_of_in_edges = nodes_with_in_edges
            .map (_._2.toList.length)
            .sum

        val out_index_key = nodes_with_out_edges.zipWithIndex.map (tup => (tup._2, tup._1))
        val in_index_key = nodes_with_in_edges.zipWithIndex.map (tup => (tup._2, tup._1))

        val average_out_edges = sum_of_out_edges / nodes_with_out_edges.count
        val average_in_edges = sum_of_in_edges / nodes_with_in_edges.count

        val midpoint_out = (nodes_with_out_edges.count / 2).toInt
        val midpoint_in = (nodes_with_in_edges.count / 2).toInt

        println("Average out-edges: " + average_out_edges)
        println("Average in-edges: " + average_in_edges)
        println("Median out-edges: " + out_index_key.lookup(midpoint_out)(0)._2.toList.length)
        println("Median in-edges: " + in_index_key.lookup(midpoint_in)(0)._2.toList.length)
    }
}