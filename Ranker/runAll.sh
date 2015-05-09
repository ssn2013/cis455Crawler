cd /home/sruthi/Desktop/theNEWdb/worker1
rm -rf spoolIn spoolOut; mkdir spoolIn spoolOut
rm output*
echo "removed from worker1: "`ls`

cd /home/sruthi/Desktop/theNEWdb/worker2
rm -rf spoolIn spoolOut; mkdir spoolIn spoolOut
rm output*
echo "removed frm worker2: "`ls`

cd /home/sruthi/Desktop/cis455Proj/Ranker

sh stopAll.sh ; ./deploy.sh ; sh startAll.sh
