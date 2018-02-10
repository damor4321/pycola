<?

$path = '/tmp/damor/queue/';
$urls = array('http://services1.com/f5.php', 'http://189.167.46.140/', 'http://10.20.20.51/'); //urls that response http queries
$urls = array('http://2.2.2.2/'); //urls that doesn't exist
$urls = array('http://cms.jmeservicios.com/f5.php'); //url that response http queries
$urls = array('http://services2.com/test_queue_call.php'); //url that response http queries
$urls = array('http://services3.com'); //urls that response http queries

function dated_path($path, $filename, $interval=1) {
        
  $path .= (substr($path, -1) != '/')? '/': '';
  $int = (int)(time() / (60*$interval)) * (60*$interval);
        
  return sprintf("%s%s/%s/%s/%s-%s", 
    $path, 
    $int, 
    substr($filename, 0, 2), 
    substr($filename, 2, 2), 
    microtime(true), 
    $filename);
}

for ($i = 0; $i < 10000; $i++) {
    $item = md5(microtime().rand(10000, 99999));
    $time = time() % 60;
    
    $n_path = dated_path($path, $item, 1);
    
    var_dump($n_path);
    @mkdir(dirname($n_path), 0777, true);
    
    $c = $urls[rand(0, count($urls)-1)] ."?diff=" . urlencode(microtime());
    fwrite(fopen($n_path, 'w+'), $c, strlen($c));
}

?>
