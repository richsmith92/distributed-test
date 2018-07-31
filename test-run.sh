TIMEFORMAT='in %Rs'
cabal build && {
	for i in $(grep -o '.$' nodes.txt); do
		{
			time ./dist/build/testnode/testnode -i $i -s1 -w1 2>/dev/shm/$i
		} 2>&1 | sed -n 'N;s/\n/\t/;p' & # join command and `time` output lines
	done
	echo 'Started nodes.'
}
