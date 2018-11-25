import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Edge, Graph, VertexId}


object TP_graph {

  case class User(name: String, age : Int, job : String, sex : String)

  def main(args: Array[String]) : Unit = {

    val conf = new SparkConf()
      .setAppName("Stats Graph")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger //import de org.apache.log4j
    rootLogger.setLevel(Level.ERROR) // pour éviter spark trop verbeux

    val nodes : RDD[(VertexId, User)] =
      sc.textFile("./src/main/ressources/users.txt")
        .filter(!_.startsWith("#"))
        .map{line =>
          val row = line split '\t'
          (row(0).toLong, User(row(1).toString, row(2).toInt, row(3).toString,
            row(4)))
        }


    val links : RDD[Edge[String]] =  sc.textFile("./src/main/ressources/relationships.txt")
        .filter(!_.startsWith("#"))
        .map { line =>
          val row = line split '\t'
          Edge(row(0).toLong, row(1).toLong, row(2).toString)
        }

    val graph = Graph(nodes, links)

    //  1 Lister les personnes

    graph.vertices.collect.map(p => p._2).foreach(println)

   // graph.vertices.map(v => v._1.name).collect.foreach(println);

    // 2 l'age de l'utilisateur 8
    println("L'age de l'utilistaeur 8 est :")
    graph.vertices.filter(v => v._1 == 8 ).map(v => v._2.age).collect.foreach(println)

    // 3 la liste des professeurs
    val profs = graph.vertices.filter(v => v._2.job == "teacher" )

    println("le nombre de professeurs est :" + profs.count())

    //4 Donner l’âge moyen des personnes

    val AgeMoyenpersonne = graph.vertices.map(v =>v._2.age).sum() / graph.vertices.count()

    println("L'age moyen des personnes est :"+AgeMoyenpersonne)

    //5 Donner le(s) nom(s) et l’âge de la/les personne(s) la/les moins âgé(es)
    val AgeMin  =graph.vertices.map(v =>v._2.age).min()
    val Pers = graph.vertices.filter(v => v._2.age == AgeMin ).map(v => (v._2.name,v._2.age) )
    println("Nom , Age")
    Pers.foreach(println)


    //6 Donner le nombre de relations de travail de type studentOf
    val relationStudentOf = graph.edges.filter(x => x.attr =="studentOf").count()
    println("le nombre de relations de travail de type studentOf est :"+ relationStudentOf)


    //7 Donner le nombre moyen de professeur par élève

    val nmbreProfParEtudiant = graph.edges.filter(e => e.attr =="studentOf").groupBy(e => e.srcId).mapValues(_.size)

    val NmbreMoyenProfParEtudiant = nmbreProfParEtudiant.map(n => n._2).sum().toDouble / nmbreProfParEtudiant.count().toDouble

    println("Le nombre moyen de profésseurs par élève est :"+ NmbreMoyenProfParEtudiant)


    // 8 Donner le nombre de personnes pour chacun des sexes
    println("le nombre de personnes pour chacun des sexes est :")
    graph.vertices.groupBy(v => v._2.sex).mapValues(_.size).foreach(println)


    //• 9 Donner le nombre d’arc dans le graph composé uniquement de femmes
      val nombreArcFemmes = graph.triplets.filter(r => (r.srcAttr.sex =="f" && r.dstAttr.sex =="f"  )).count()
      println("le nombre d’arc dans le graph composé uniquement de femmes est : "+ nombreArcFemmes )

    //• 10 Donner le nombre de composantes connexes et leur tailles
      val nombreComposantConexe = graph.connectedComponents().vertices.groupBy(p=>p._2).mapValues(_.size)
        println("le nombre de composants connexes et leur taille est : " +nombreComposantConexe.count())
        println("leur taille est : ")
        nombreComposantConexe.map(p=>p._2).foreach(println)


    //  Partie 2

    //11 les personnes qui ne sont pas professeurs
      val NotProf = graph.vertices.filter(p => p._2.job != "teacher").map(p => p._2)
        println("Les personnes qui ne sont pas de profésseurs sont : ")
        NotProf.foreach(println)

    // 12 Donnes le nombre de relation par type pour Charlie
    val RelationParType = graph.triplets.filter(r => (r.srcAttr.name =="charlie" || r.dstAttr.name =="charlie" )).groupBy(r => r.attr).mapValues(_.size)
     println("le nombre de relation par type de charlie : ")
      RelationParType.foreach(println)

    // 13 la valeur du degret maximal dans le graph des personnes de moins de 30 ans

    val newEdges =  graph.triplets.filter(r => (r.srcAttr.age <=30 && r.dstAttr.age <=30)).map(e => Edge(e.srcId, e.dstId, e.attr))
    val newVertexId = graph.vertices.filter(r => (r._2.age <=30 ))
    val MaxDegreeGraphMoin30 = Graph(newVertexId, newEdges).degrees.map(r  => r._2).max()

    println("la valeur du degret maximal dans le graph des personnes moins de 30 est : "+ MaxDegreeGraphMoin30)

    //14  Le nombre moyen de contact par personne pour chaque  composant connexe
    val NombrePersonneComposant1 = graph.connectedComponents().vertices.groupBy(c => c._2 ).mapValues(_.map(_._1)).map(_._2).max()
    val NombrePersonneComposant01 = NombrePersonneComposant1.toList
    val NbreContactComp1 = graph.degrees.collect.filter(c =>  NombrePersonneComposant01.contains(c._1)).map(c =>c._2 )
    val NombreMoyenContactComp1 = NbreContactComp1.sum.toDouble / NbreContactComp1.length.toDouble
    println("le nombre moyen de contact par personne  pour le premier composant est  "+NombreMoyenContactComp1)


    val NombrePersonneComposant2 = graph.connectedComponents().vertices.groupBy(c => c._2 ).mapValues(_.map(_._1)).map(_._2).min()
    val NombrePersonneComposant02 = NombrePersonneComposant2.toList
    val NbreContactComp2 = graph.degrees.collect.filter(c =>  NombrePersonneComposant02.contains(c._1)).map(c =>c._2 )
    val NombreMoyenContactComp2 = NbreContactComp2.sum.toDouble / NbreContactComp2.length.toDouble
    println("le nombre moyen de contact par personne  pour le deuxième composant est  "+ NombreMoyenContactComp2)


    //15 les noms des personnes se trouvant en distance de 2 de marion
    val IdMarion = graph.vertices.filter(p => p._2.name =="marion").map(p => p._1).first()
    val  graphtest = graph.mapEdges(e => 1)
    val initialGraph = graphtest.mapVertices((id, _) =>
     if (id == IdMarion) 0.0 else Double.PositiveInfinity)

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )

    val LesId = sssp.vertices.collect.filter(p  => p._2 == 2.0).map(p  => p._1)
    val lesnoms = graph.vertices.filter(p  => LesId.contains(p._1)).map(p  => p._2.name)

    println("les noms des personnes se trouvant en distance de 2 de marion sont :")

    lesnoms.foreach(println)

  }
}
