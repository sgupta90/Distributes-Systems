namespace java edu.umn
typedef i64 int

struct DHTNode{
	1:string ip,
	2:int port,
	3:int id,
}

struct DHTNodePath{
	1: DHTNode node,
	2: list<DHTNode> visitedNodes,
}

service DHTNodeService{
	string getBook(1:string bookTitle, 2:int debugFlag),
	void setBook(1:string bookTitle, 2:string genre, 3:int debugFlag),
	void updateFingerTable(1:DHTNode newNode, 2:i32 i,3:list<DHTNode> visitedNodes),
	DHTNodePath findSuccessor(1:int key,2:list<DHTNode> visitedNodes),
	DHTNodePath findPredecessor(1:int key, 2:list<DHTNode> visitedNodes),
	void setPredecessor(1:DHTNode predecessor),
	map<string,string> transferKeys(1:int key),
}
