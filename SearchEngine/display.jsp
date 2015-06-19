



<!DOCTYPE HTML>
<!--
	Prologue by HTML5 UP
	html5up.net | @n33co
	Free for personal and commercial use under the CCA 3.0 license (html5up.net/license)
-->
<%@ page import="java.io.BufferedReader, java.io.FileInputStream, java.io.IOException, java.io.InputStream, java.io.InputStreamReader, java.io.UnsupportedEncodingException, java.math.BigInteger, java.net.HttpURLConnection,java.net.MalformedURLException,java.net.URL,java.security.MessageDigest,java.security.NoSuchAlgorithmException,java.util.ArrayList,java.util.Collections,java.util.Comparator,java.util.HashMap,java.util.Iterator,java.util.LinkedHashMap,java.util.LinkedList,java.util.List,java.util.Map,java.util.Map.Entry,org.json.JSONArray,org.json.JSONException,org.json.JSONObject" %>

  
<html>
	<%float lati=0.0f;
	  float longi=0.0f;%>
	<head>
		<title>TOP SEARCH</title>
		<meta http-equiv="content-type" content="text/html; charset=utf-8" />
		<meta name="description" content="" />
		<meta name="keywords" content="" />
		<!--[if lte IE 8]><script src="css/ie/html5shiv.js"></script><![endif]-->
		--<script src="js/jquery.min.js"></script>
		<script src="js/jquery.scrolly.min.js"></script>
		<!--<script src="js/jquery.scrollzer.min.js"></script>-->
		<script src="js/skel.min.js"></script>
		<script src="js/skel-layers.min.js"></script>
		<script src="js/init.js"></script>
		<script type="text/javascript">
  function transcribe(words) {
  document.getElementById("speech").value = words;
  document.getElementById("mic").value = "";
  document.getElementById("speech").focus();
}
  
</script>

<script>
		amzn_assoc_ad_type = "responsive_search_widget";
		amzn_assoc_tracking_id = "amawid-21";
		amzn_assoc_marketplace = "amazon";
		amzn_assoc_region = "US";
		amzn_assoc_placement = "";
		amzn_assoc_search_type = "search_widget";
		amzn_assoc_width = "auto";
		amzn_assoc_height = "auto";
		amzn_assoc_default_search_category = "";
		amzn_assoc_default_search_key = "";
		amzn_assoc_theme = "light";
		amzn_assoc_bg_color = "ffffff";
		function search() {
			var queryString = document.getElementById("searchbox").value;
			//alert(queryString);
			amzn_assoc_default_search_key = "asd";
			document.getElementById("quer").value="";
			document.getElementById("output_frame").src = "about:blank";
			if(queryString.indexOf("weather") > -1 ) {
			
			 var words=queryString.split();
			 
			 if(words[0]!="weather") {
			 document.getElementById("quer").value=words[0];
			 
			}
			document.forms["weather"].submit();
			}
			
			
			
			
			xmlhttp=new XMLHttpRequest();
			url="/search/search?query="+queryString;
			
     		
 			xmlhttp.onreadystatechange=function() {
 				
 				if (xmlhttp.readyState==4 && xmlhttp.status==200) {
 					
 					if(xmlhttp.responseText.indexOf("Sorry") > -1){
 						var r = xmlhttp.responseText.split("$");
 						if(r[1] == "true"){
							htmlDoc="</br><p><i> Did you mean: "+r[2]+" ?</i></p><p><h2>Sorry no results found !</h2></p>";
 						}else{
 							htmlDoc="<p><h2>Sorry no results found !</h2></p>";
 						}
 						document.getElementById("tabs-web").innerHTML=htmlDoc;
					}
					
					else{
						var htmlDoc="</br></br><h3>Results !!</h3>"
							
					var responseObj = JSON.parse(xmlhttp.responseText); 
					
					var searchItems = responseObj.result;
				
					var temp = responseObj.spellCheck; 
				
					
					if(temp == "true"){
						
						htmlDoc+="<p><i> Did you mean: "+responseObj.corrected+" ?</i></p>";
					}
					//code to display data
					var boldQuery = responseObj.corrected.split(" ");
					for(var i = 0; i < searchItems.length; i++) {
						var snippet = searchItems[i].snippet;
						var displayUrl = searchItems[i].url.replace("http://","").replace("https://");
						var mainDisplay = displayUrl.split("/")[0];
						htmlDoc+="<p font-size: \"small\" line-height: '10%' >";
						htmlDoc+="<a href=\""+searchItems[i].url+"\" target=\"_blank\" font-size:\"x-large\" text-decoration: \"underline\">";	
						if(searchItems[i].title == ""){

							htmlDoc+="<u><font size=\"5\" color=\"blue\">"+mainDisplay+"</font></u></a><br/>";

							}else{

							    htmlDoc+="<u><font size=\"5\" color=\"blue\">"+searchItems[i].title+"</font></u></a><br/>";

							}
						for(var j=0;j< boldQuery.length;j++){

						    snippet = highlightWords(snippet, boldQuery[j]);

						    }
    					htmlDoc+="<font size=\"3\" color=\"green\">"+displayUrl+"</font><br/>";
    					htmlDoc+="<font size=\"3\" >" +snippet+"</font><br/>";
    					
 					}
						
						document.getElementById("tabs-web").innerHTML=htmlDoc;
 					}
 				}
 				}

 				xmlhttp.open("GET",url,true);
 				xmlhttp.send();
		}
		
		function highlightWords( line, word )

		{

		    var regex = new RegExp( '(' + word + ')', 'gi' );

		    return line.replace( regex, "<b><i><u>$1</u></i></b>" );

		}
		</script>
		<noscript>
			<link rel="stylesheet" href="css/skel.css" />
			<link rel="stylesheet" href="css/style.css" />
			<link rel="stylesheet" href="css/style-wide.css" />
		</noscript>
		<link rel="stylesheet" href="//code.jquery.com/ui/1.11.4/themes/smoothness/jquery-ui.css">
		<script src="//code.jquery.com/jquery-1.10.2.js"></script>
  <script src="//code.jquery.com/ui/1.11.4/jquery-ui.js"></script>
  
  
  <script>
  $(function() {
    $( "#tabs" ).tabs();
  });
  </script>
  
<style>
	#tabs-web {
	height: 500px;
	margin: 0px;
	padding: 0px
}
	#tabs-images {
	height: 500px;
	margin: 0px;
	padding: 0px
	}
	#tabs-videos {
	height: 500px;
	margin: 0px;
	padding: 0px
	}
</style>
  <script
	src="https://maps.googleapis.com/maps/api/js?v=3.exp&signed_in=true&libraries=visualization"></script>

  
  

		<!--[if lte IE 9]><link rel="stylesheet" href="css/ie/v9.css" /><![endif]-->
		<!--[if lte IE 8]><link rel="stylesheet" href="css/ie/v8.css" /><![endif]-->
	</head>
	<body>
	
	
	
		<!-- Header -->
			<div id="header" class="skel-layers-fixed">

				<div class="top">

					<!-- Logo -->
						<div id="logo">
							<span class="image avatar48"><img src="images/Hyperspace.jpg" alt="" /></span>
							<h1 id="title">TOP SEARCH</h1><br\>
							<p>Adarsh,Aryaa,Deepak,Sruthi</p>
							
						</div>
			
						<div id="map">
						
						<script src="//z-eu.amazon-adsystem.com/widgets/q?ServiceVersion=20070822&Operation=GetScript&ID=OneJS&WS=1&MarketPlace=US"></script>
						
						</div> 
						
			

					<!-- Nav -->
						<nav id="nav">
					</br>
					<iframe name="output_frame" id="output_frame" style="width:100%"></iframe>
					<form id="weather" action="http://www.wunderground.com/cgi-bin/findweather/getForecast" target="output_frame">
					<input type=hidden id="quer" name=query size=33>
					</form>


 
							<!--
							
								Prologue's nav expects links in one of two formats:
								
								1. Hash link (scrolls to a different section within the page)
								
								   <li><a href="#foobar" id="foobar-link" class="icon fa-whatever-icon-you-want skel-layers-ignoreHref"><span class="label">Foobar</span></a></li>

								2. Standard link (sends the user to another page/site)

								   <li><a href="http://foobar.tld" id="foobar-link" class="icon fa-whatever-icon-you-want"><span class="label">Foobar</span></a></li>
							
							-->
						
						</nav>
						
				</div>
				
				<div class="bottom">

					<!-- Social Icons -->
						<ul class="icons">
							<li><a href="#" class="icon fa-twitter"><span class="label">Twitter</span></a></li>
							<li><a href="#" class="icon fa-facebook"><span class="label">Facebook</span></a></li>
							<li><a href="#" class="icon fa-github"><span class="label">Github</span></a></li>
							<li><a href="#" class="icon fa-dribbble"><span class="label">Dribbble</span></a></li>
							<li><a href="#" class="icon fa-envelope"><span class="label">Email</span></a></li>
						</ul>
				
				</div>
			
			</div>

		<!-- Main -->
		<section id="top" class="one dark cover">

			
			<div class="top-searchbar" align="center">
					<link rel="stylesheet" href="css/speech-input.css">
					<style>
						.si-wrapper input {
							font-size: 1em;
							padding: .1em;
						}
					</style>
				
					<div class="si-wrapper">
    					<input id="searchbox" type="text"  class="si-input" value='Top Search' onfocus="this.value = this.value=='Top Search'?'':this.value;" onblur="this.value = this.value==''?'Top Search':this.value;"/>
    					<button class="si-btn">
        				speech input
        				<span class="si-mic"></span>
        				<span class="si-holder"></span>
    					</button>
    					

					</div>
					<script src="js/speech-input.js"></script>
						
					
				
							  <input type="submit" src="images/pic02.jpg" onclick="search()"/> 

			</div>
				
									
				
			
		</section>
			<div id="main" style=" margin-left:24%;width:auto">
				<div id="tabs" style=" margin-left:-1%">
  			<ul>
    			<li><a href="#tabs-web">Web</a></li>
    			<li><a href="#tabs-images" onclick="">Images</a></li>
  				  <li><a href="#tabs-videos" onclick="">Videos</a></li>
  			</ul>
 			 <div id="tabs-web">
    
    			<!-- <p>Heat Map</p> -->
  			</div>
  			<div id="tabs-images">
  			<p>Sorry...This feature is not available</p>
  			
  			
    			<!-- <p>Details</p> -->
  			</div>
  			<div id="tabs-videos">
    			<p>Sorry...This feature is not available</p>
  			</div>
   			</div>
			</div>
			</div>
   		</body>
</html>

