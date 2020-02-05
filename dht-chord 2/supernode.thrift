namespace java edu.umn
typedef i32 int

service SuperNodeService{
	string join(1:string ip, 2:int port),
	void postJoin(1:string ip, 2:int port),
	string getNode()
}
