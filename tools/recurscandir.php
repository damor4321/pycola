<?
        $data = array();


        $range = array_diff(scandir("/tmp/damor/queue2"), array('.', '..'));
        sort($range);

        

        $limit = 20;
        
        foreach ((array)$range as $interval) {
           
            $iter = new RecursiveDirectoryIterator("/tmp/damor/queue2/".$interval);

            foreach (new RecursiveIteratorIterator($iter) as $filename => $obj) {
                $mtime = $obj->getMTime().sha1($filename);
                $data[$mtime] = $filename;
           
                   if (--$limit <= 0) break 2;
            }



            
        } 

        ksort($data);

    print_r ($data);
    echo "\n\n\n";
    foreach ((array)$data as $file) { echo "$file\n"; }

?>
