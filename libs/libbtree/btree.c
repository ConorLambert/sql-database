#include "btree.h"

typedef enum {left = -1,right = 1} position_t;

//void print_single_node(btree *btree, bt_node * node);
static bt_node * allocate_btree_node (unsigned int order);
static int free_btree_node (bt_node * node);

static node_pos get_btree_node(btree * btree, node_pos *starting_node_pos, void * key);

static int delete_key_from_node(btree * btree, node_pos * node_pos);
static bt_node * merge_nodes(btree * btree, bt_node * n1, bt_key_val * kv ,bt_node * n2);
static void move_key(btree * btree, bt_node * node, unsigned int index, position_t pos);
static node_pos get_max_key_pos(btree * btree, bt_node * subtree);
static node_pos get_min_key_pos(btree * btree, bt_node * subtree);
static bt_node * merge_siblings(btree * btree, bt_node * parent, unsigned int index, position_t pos);
static void copy_key_val(btree * btree,bt_key_val * src, bt_key_val * dst);

int deepCopy(node_pos* destination_node_pos, node_pos* source_node_pos) {
	destination_node_pos->node = source_node_pos->node;
        destination_node_pos->index = source_node_pos->index;
        destination_node_pos->child_pos = source_node_pos->child_pos;
        destination_node_pos->number_of_possible_children = source_node_pos->number_of_possible_children;
	destination_node_pos->parent = source_node_pos->parent;
        destination_node_pos->child = source_node_pos->child;
	destination_node_pos->key_val = source_node_pos->key_val;
	return 0;
}

node_pos *create_node_pos() {
	node_pos *node_position = malloc(sizeof(node_pos));
        node_position->node = NULL;
        node_position->index = 0;
        node_position->child_pos = 0;
        node_position->number_of_possible_children = 0;
	node_position->parent = NULL;
        node_position->child = NULL;
	node_position->key_val = NULL;
	return node_position;
}

int destroy_node_pos(node_pos *node_pos) {
	if(!node_pos){
		return -1;
	} else {
		node_pos->node = NULL;
	        node_pos->parent = NULL;
        	node_pos->child = NULL;
		node_pos->key_val = NULL;
		free(node_pos);
		return 0;
	}
}


/**
*	Used to create a btree with just the root node
*	@param order The order of the B-tree
*	@return The an empty B-tree
*/
btree * btree_create(unsigned int order) {
	btree * btree;
	btree = mem_alloc(sizeof(*btree));
	btree->order = order;
	btree->root = allocate_btree_node(order);
	btree->root->leaf = true;
	btree->root->nr_active = 0;
	btree->root->next = NULL;
	btree->root->level = 0;
	return btree;
}

/**
*       Function used to allocate memory for the btree node
*       @param order Order of the B-Tree
*	@param leaf boolean set true for a leaf node
*       @return The allocated B-tree node
*/
static bt_node * allocate_btree_node (unsigned int order) {
        bt_node * node;
	
        // Allocate memory for the node
        node = (bt_node *)mem_alloc(sizeof(bt_node));
       	
        // Initialize the number of active nodes
        node->nr_active = 0;
	
        // Initialize the keys
        node->key_vals = (bt_key_val **)mem_alloc(2*order*sizeof(bt_key_val*) - 1);
	        
        // Initialize the child pointers
        node->children = (bt_node **)mem_alloc(2*order*sizeof(bt_node*));

	// Use to determine whether it is a leaf
	node->leaf = true;
	
	// Use to determine the level in the tree
	node->level = 0;
	
	//Initialize the linked list pointer to NULL
	node->next = NULL;
	
        return node;
}

/**
*       Function used to free the memory allocated to the b-tree 
*       @param node The node to be freed
*       @param order Order of the B-Tree
*       @return The allocated B-tree node
*/
static int free_btree_node (bt_node * node) {

        mem_free(node->children);
        mem_free(node->key_vals);
        mem_free(node);

        return 0;
}

/**
*	Used to split the child node and adjust the parent so that
*	it has two children
*	@param parent Parent Node
*	@param index Index of the child node
*	@param child  Full child node
*	
*/
static void btree_split_child(btree * btree, bt_node * parent, 
				unsigned int index,
				bt_node * child) {
	int i = 0;	
	unsigned int order = btree->order;

	bt_node * new_child = allocate_btree_node(btree->order); 
	new_child->leaf = child->leaf;
	new_child->level = child->level;
	new_child->nr_active = btree->order - 1;

	// Copy the higher order keys to the new child	
	for(i=0;i<order - 1;i++) {
		new_child->key_vals[i] = child->key_vals[i + order];
		if(!child->leaf) {
			new_child->children[i] = 
				child->children[i + order];
		} 
	}
	
	// Copy the last child pointer
	if(!child->leaf) {
		new_child->children[i] = 
		child->children[i + order];
	}

	child->nr_active = order - 1;
	
	for(i = parent->nr_active + 1;i > index + 1;i--) {
		parent->children[i] = parent->children[i - 1];
	}
	parent->children[index + 1] = new_child;

	for(i = parent->nr_active;i > index;i--) {
		parent->key_vals[i] = parent->key_vals[i - 1];
	}

	parent->key_vals[index] = child->key_vals[order - 1];	
	parent->nr_active++;
}

/**
*	Used to insert a key in the non-full node
*	@param btree The btree
*	@param node The node to which the key will be added
*	@param the key value pair
*	@return void
*/

static void btree_insert_nonfull (btree * btree, bt_node * parent_node,
				bt_key_val * key_val) {
	
	unsigned int key = btree->value(key_val->key);
	int i ;
	bt_node * child;	
	bt_node * node = parent_node;

insert:	i = node->nr_active - 1;
	if(node->leaf) {
		while(i >= 0 && key < btree->value(node->key_vals[i]->key)) {
			node->key_vals[i + 1] = node->key_vals[i];
			i--;
		}
		node->key_vals[i + 1] = key_val;
		node->nr_active++; 
	} else {
		while (i >= 0 && key < btree->value(node->key_vals[i]->key)) {
			i--;
		}
		i++;
		child = node->children[i]; 
		
		if(child->nr_active == 2*btree->order - 1) {
			btree_split_child(btree,node,i,child);	
			if(btree->value(key_val->key) > 
				btree->value(node->key_vals[i]->key)) {
				i++;	
			}	
		}
		node = node->children[i];
		goto insert;	
	} 
}


/**
*       Function used to insert node into a B-Tree
*       @param root Root of the B-Tree
*       @param node The node to be inserted
*       @param compare Function used to compare the two nodes of the tree
*       @return success or failure
*/
int btree_insert_key(btree * btree, bt_key_val * key_val) {
	bt_node * rnode;

	rnode = btree->root;
	if(rnode->nr_active == (2*btree->order - 1)) {
		bt_node * new_root;
		new_root = allocate_btree_node(btree->order);
		new_root->level = btree->root->level + 1;
		btree->root = new_root;	
		new_root->leaf = false;
		new_root->nr_active = 0;
		new_root->children[0]  = rnode;
		btree_split_child(btree,new_root,0,rnode);
		btree_insert_nonfull(btree,new_root,key_val);	
	} else
		btree_insert_nonfull(btree,rnode,key_val);		

        return 0;
}

/**
*	Used to get the position of the MAX key within the subtree
*	@param btree The btree
*	@param subtree The subtree to be searched
*	@return The node containing the key and position of the key
*/
static node_pos get_max_key_pos(btree * btree, bt_node * subtree) {
	node_pos node_pos;
	bt_node * node = subtree; 

	while(true) {
		if(node == NULL) {
			break;
		}

		if(node->leaf) {
			node_pos.node 	= node;
			node_pos.index 	= node->nr_active - 1;
			return node_pos;	
		} else {
			node_pos.node 	= node;
			node_pos.index 	= node->nr_active - 1;
			node = node->children[node->nr_active];	
		}
	}
	return node_pos;
}

/**
*	Used to get the position of the MAX key within the subtree
*	@param btree The btree
*	@param subtree The subtree to be searched
*	@return The node containing the key and position of the key
*/
static node_pos get_min_key_pos(btree * btree, bt_node * subtree) {
	node_pos node_pos;
	bt_node * node = subtree; 

	while(true) {
		if(node == NULL) {
			break;
		}

		if(node->leaf) {
			node_pos.node 	= node;
			node_pos.index 	= 0;
			return node_pos;	
		} else {
			node_pos.node 	= node;
			node_pos.index 	= 0;
			node = node->children[0];	
		}
	}
	return node_pos;
}

/**
*	Merge nodes n1 and n2 (case 3b from Cormen)
*	@param btree The btree 
*	@param node The parent node 
*	@param index of the child
*	@param pos left or right
*	@return none 
*/
static bt_node * merge_siblings(btree * btree, bt_node * parent, unsigned int index , 
					position_t pos) {
	unsigned int i,j;
	bt_node * new_node;
	bt_node * n1, * n2;
        
        if (index == (parent->nr_active)) {   
               index--;
	       n1 = parent->children[parent->nr_active - 1];
	       n2 = parent->children[parent->nr_active];
        } else {
               n1 = parent->children[index];
	       n2 = parent->children[index + 1];
        }

	//Merge the current node with the left node
	new_node = allocate_btree_node(btree->order);
	new_node->level = n1->level;
	new_node->leaf = n1->leaf;

	for(j=0;j<btree->order - 1; j++) {
		new_node->key_vals[j] =	n1->key_vals[j];
		new_node->children[j] =	n1->children[j];
	}
	
	new_node->key_vals[btree->order - 1] =	parent->key_vals[index];
	new_node->children[btree->order - 1] =	n1->children[btree->order - 1];

	for(j=0;j<btree->order - 1; j++) {
		new_node->key_vals[j + btree->order] = 	n2->key_vals[j];
		new_node->children[j + btree->order] = 	n2->children[j];
	}
	new_node->children[2*btree->order - 1] = n2->children[btree->order - 1];

	parent->children[index] = new_node;

	for(j = index;j<parent->nr_active;j++) {
		parent->key_vals[j] = parent->key_vals[j + 1];
		parent->children[j + 1] = parent->children[j + 2];
	}

	new_node->nr_active = n1->nr_active + n2->nr_active + 1;
	parent->nr_active--;

	for(i=parent->nr_active;i < 2*btree->order - 1; i++) {
		parent->key_vals[i] = NULL; 
	}

	free_btree_node(n1);
	free_btree_node(n2);

	if (parent->nr_active == 0 && btree->root == parent) {
		free_btree_node(parent);
		btree->root = new_node;
		if(new_node->level)
			new_node->leaf = false;
		else
			new_node->leaf = true; 
	}

	return new_node;
}

/**
*	Move the key from node to another	
*	@param btree The B-Tree
*	@param node The parent node
*	@param index of the key to be moved done 
*	@param pos the position of the child to receive the key 
*	@return none
*/
static void move_key(btree * btree, bt_node * node, unsigned int index, position_t pos) {
	bt_node * lchild;
	bt_node * rchild;
	unsigned int i;

	if(pos == right) {
		index--;
	}
	lchild = node->children[index];
	rchild = node->children[index + 1];
	
	// Move the key from the parent to the left child
	if(pos == left) {
		lchild->key_vals[lchild->nr_active] = node->key_vals[index];
		lchild->children[lchild->nr_active + 1] = rchild->children[0];
		rchild->children[0] = NULL;
		lchild->nr_active++;

		node->key_vals[index] = rchild->key_vals[0];
		rchild->key_vals[0] = NULL;

		for(i=0;i<rchild->nr_active - 1;i++) {
			rchild->key_vals[i] = rchild->key_vals[i + 1];
			rchild->children[i] = rchild->children[i + 1];
		}
		rchild->children[rchild->nr_active - 1] = 
				rchild->children[rchild->nr_active];
		rchild->nr_active--;
	} else {
		// Move the key from the parent to the right child
		for(i=rchild->nr_active;i > 0 ; i--) {
			rchild->key_vals[i] = rchild->key_vals[i - 1];
			rchild->children[i + 1] = rchild->children[i];		
		}
		rchild->children[1] = rchild->children[0];		
		rchild->children[0] = NULL;

		rchild->key_vals[0] = node->key_vals[index];

		rchild->children[0] = lchild->children[lchild->nr_active];	
		lchild->children[lchild->nr_active] = NULL;

		node->key_vals[index] = lchild->key_vals[lchild->nr_active - 1];
		lchild->key_vals[lchild->nr_active - 1] = NULL;
			
		lchild->nr_active--;
		rchild->nr_active++;		
	}				
}

/**
*	Merge nodes n1 and n2
*	@param n1 First node
*	@param n2 Second node
*	@return combined node
*/
static bt_node * merge_nodes(btree * btree, bt_node * n1, bt_key_val * kv,
                                                bt_node * n2) {
	bt_node * new_node;
	unsigned int i;	

	new_node = allocate_btree_node(btree->order);
	new_node->leaf = true;
	
	for(i=0;i<n1->nr_active;i++) {
		new_node->key_vals[i]   = n1->key_vals[i];	
                new_node->children[i]   = n1->children[i];
	}
        new_node->children[n1->nr_active] = n1->children[n1->nr_active];
        new_node->key_vals[n1->nr_active] = kv;

	for(i=0;i<n2->nr_active;i++) {
		new_node->key_vals[i + n1->nr_active + 1] = n2->key_vals[i];
                new_node->children[i + n1->nr_active + 1] = n2->children[i];
	}
        new_node->children[2*btree->order - 1] = n2->children[n2->nr_active];
	
        new_node->nr_active = n1->nr_active + n2->nr_active + 1;
        new_node->leaf = n1->leaf;
        new_node->level = n1->level;

	free_btree_node(n1);
	free_btree_node(n2);

	return new_node;
}

/**
*	Used to delete a key from the B-tree node
*	@param btree The btree
*	@param node The node from which the key is to be deleted 	
*	@param key The key to be deleted
*	@return 0 on success -1 on error 
*/

int delete_key_from_node(btree * btree, node_pos * node_pos) {
	unsigned int keys_max = 2*btree->order - 1;
	unsigned int i;
	bt_key_val * key_val;
	bt_node * node = node_pos->node;

	if(node->leaf == false) {
		return -1;
	}	
	
	key_val = node->key_vals[node_pos->index];
	//printf("\nKEY_VAL->key %s\n", btree->value(node->key_vals[i]->key));

	for(i=node_pos->index;i< keys_max - 1;i++) {
		node->key_vals[i] = node->key_vals[i + 1];	
	}
	
	if(key_val->key) {
		mem_free(key_val->key);
                key_val->key = NULL;
	}

	if(key_val->val) {
		mem_free(key_val->val);
                key_val->val = NULL;
	}
	
	node->nr_active--;

	if(node->nr_active == 0 ) {
		free_btree_node(node);
	}
	return 0;
}

/**
*       Function used to delete a node from a  B-Tree
*       @param btree The B-Tree
*       @param key Key of the node to be deleted
*       @param value function to map the key to an unique integer value        
*       @param compare Function used to compare the two nodes of the tree
*       @return success or failure
*/

int btree_delete_key(btree * btree,bt_node * subtree,void * key) {
	unsigned int i,index;
	bt_node * node = NULL, * rsibling, *lsibling;
	bt_node * comb_node, * parent;
	node_pos sub_node_pos;
	node_pos node_pos;
	bt_key_val * key_val, * new_key_val;
	unsigned int kv = btree->value(key);	

	node = subtree;
	parent = NULL;	
del_loop:for (i = 0;;i = 0) {
             
            //If there are no keys simply return
            if(!node->nr_active)
                return -1;
	    // Fix the index of the key greater than or equal
	    // to the key that we would like to search
		
	    while (i < node->nr_active && kv > 
			    btree->value(node->key_vals[i]->key) ) {
		    i++;
	    }
	    index = i;
	
	    // If we find such key break		    
	    if(i < node->nr_active && kv == btree->value(node->key_vals[i]->key)) {
			break;
	    }
            if(node->leaf)
                return -1;
    	    //Store the parent node
	    parent = node;

	    // To get a child node 
	    node = node->children[i];

            //If NULL not found
            if (node == NULL)
                return -1;

            if (index == (parent->nr_active)) {   
                lsibling =  parent->children[parent->nr_active - 1];
	    	rsibling = NULL; 
            } else if (index == 0) {
                lsibling = NULL;
                rsibling = parent->children[1];
            } else {
        	lsibling = parent->children[i - 1];
	    	rsibling = parent->children[i + 1];
            }
	    if (node->nr_active == btree->order - 1 && parent) {
		// The current node has (t - 1) keys but the right sibling has > (t - 1)
		// keys
		if (rsibling && (rsibling->nr_active > btree->order - 1)) {
			move_key(btree,parent,i,left);
		} else
		// The current node has (t - 1) keys but the left sibling has (t - 1)
		// keys
		if (lsibling && (lsibling->nr_active > btree->order - 1)) {
			move_key(btree,parent,i,right);
		} else
		// Left sibling has (t - 1) keys
	        if(lsibling && (lsibling->nr_active == btree->order - 1)) {
		        node = merge_siblings(btree,parent,i,left);
		} else
                // Right sibling has (t - 1) keys
	        if(rsibling && (rsibling->nr_active == btree->order - 1)) {
		        node = merge_siblings(btree,parent,i,right);
		}
	    }
        }            
	
	//Case 1 : The node containing the key is found and is the leaf node.
	//Also the leaf node has keys greater than the minimum required.
	//Simply remove the key
	if(node->leaf && (node->nr_active > btree->order - 1)) {
		print_subtree(btree, node);
		node_pos.node = node;
		node_pos.index = index;
		delete_key_from_node(btree,&node_pos);
		return 0;
	}
	
	//If the leaf node is the root permit deletion even if the number of keys is
	//less than (t - 1)
	if(node->leaf && (node == btree->root)) {
		print_subtree(btree, node);
		node_pos.node = node;
		node_pos.index = index;
		delete_key_from_node(btree,&node_pos);
		return 0;
	}

	//Case 2: The node containing the key is found and is an internal node
	if(node->leaf == false) {
		if(node->children[index]->nr_active > btree->order - 1 ) {
			sub_node_pos = get_max_key_pos(btree,node->children[index]);
                        key_val = sub_node_pos.node->key_vals[sub_node_pos.index];
		        new_key_val = (bt_key_val *)mem_alloc(sizeof(bt_key_val));
                        copy_key_val(btree,key_val,new_key_val);
        		node->key_vals[index] = new_key_val;	
               
                        btree_delete_key(btree,node->children[index],key_val->key);
			if(sub_node_pos.node->leaf == false) {
                                print("Not leaf\n");
                        }
		} else if ((node->children[index + 1]->nr_active > btree->order - 1) ) {
			sub_node_pos = get_min_key_pos(btree,node->children[index + 1]);
                        key_val = sub_node_pos.node->key_vals[sub_node_pos.index];
                        new_key_val = (bt_key_val *)mem_alloc(sizeof(bt_key_val));
		        copy_key_val(btree,key_val,new_key_val);
			node->key_vals[index] = new_key_val;	
	
                        btree_delete_key(btree,node->children[index + 1],key_val->key);
			if(sub_node_pos.node->leaf == false) {
                                print("Not leaf\n");
                        }

		} else if ( 
			node->children[index]->nr_active == btree->order - 1 &&
			node->children[index + 1]->nr_active == btree->order - 1) {
			comb_node = merge_nodes(btree,node->children[index],
                                node->key_vals[index],
				node->children[index + 1]);
			node->children[index] = comb_node;
			
			for(i=index + 1;i<node->nr_active;i++) {
				node->children[i] = node->children[i + 1];
				node->key_vals[i - 1] = node->key_vals[i];
			}
			node->nr_active--;
                        if (node->nr_active == 0 && btree->root == node) {
                                free_btree_node(node);
                                btree->root = comb_node;        
                        } 
                        node = comb_node;
                        goto del_loop;
			}		
		
          }

	// Case 3:
	// In this case start from the top of the tree and continue
	// moving to the leaf node making sure that each node that
	// we encounter on the way has atleast 't' (order of the tree)
	// keys 
	if(node->leaf && (node->nr_active > btree->order - 1)) {
	      node_pos.node = node;
	      node_pos.index = index;
	      delete_key_from_node(btree,&node_pos);
	}

        return 0;
}

/**
*       Used to destory btree
*       @param btree The B-tree
*       @return none
*/
void btree_destroy(btree * btree) {
       int i = 0;
       unsigned int current_level;

       bt_node * head, * tail, * node;
       bt_node * child, * del_node;

       node = btree->root;
       current_level = node->level;
       head = node;
       tail = node;

       while(true) {
               if(head == NULL) {
                       break;
               }
               if (head->level < current_level) {
                       current_level = head->level;
               }

               if(head->leaf == false) {
                       for(i = 0 ; i < head->nr_active + 1; i++) {
                               child = head->children[i];
                               tail->next = child;
                               tail = child;
                               child->next = NULL;
                       }
               }
               del_node = head;
               head = head->next;
               free_btree_node(del_node);
       }

}


/*
	printf("\n\t\t\t\tMOVING TO ANOTHER CHILD %d\n", starting_node_pos->child_pos);
					node_pos *node_pos = create_node_pos();
					printf("\n\t\t\tnode_pos is being set to:\n");
					print_single_node(btree, node->children[starting_node_pos->child_pos]);
					node_pos->node = node->children[starting_node_pos->child_pos];
					starting_node_pos->child = node_pos;
					node_pos->parent = starting_node_pos;
					printf("\n\t\t\tCURRENT node\n");
					print_single_node(btree, starting_node_pos->node);
					node = node->children[starting_node_pos->child_pos++];
					starting_node_pos = starting_node_pos->child;
					printf("\n\t\t\tPARENT node\n");
					print_single_node(btree, starting_node_pos->parent->node); 
					if(starting_node_pos->child) {
						printf("\n\t\t\tCHILD node\n");	
						print_single_node(btree, starting_node_pos->child->node);
					}					
					kp = get_btree_node_single(btree, starting_node_pos, key);

				if(!kp) {
						printf("\n\n\nget_btree_node_single returned negative\n");
						if(starting_node_pos->parent) {
							printf("\n\t\t\tmoving to parent\n");
							starting_node_pos = starting_node_pos->parent;
							destroy_node_pos(starting_node_pos->child);
						} else {	// its the last and only node_poos but we need at least 1 active for sqlaccess
							printf("\n\t\t\tno more nodes to search\n");
							starting_node_pos->node = NULL;
							starting_node_pos->parent = NULL;
							starting_node_pos->child = NULL;
							return kp;	
						}
					} else {
						printf("\n\t\t\tget_btree_node_single returned positive, starting_node_pos->index %d\n", starting_node_pos->index);
						printf("\n\t\t\tPARENT\n");	
						print_single_node(btree, kp->parent->node);
						if(starting_node_pos->child) {
							printf("\n\t\t\tCHILD node\n");	
							print_single_node(btree, starting_node_pos->child->node);
						}					
						if(starting_node_pos->node) {
							printf("\n\t\t\tCurrent success node\n");	
							print_single_node(btree, starting_node_pos->node);
						}					
		
						//starting_node_pos->node = NULL;
						return kp;
					}
				} else {	// we have not found the key so return NULL
					// go back to the parent node_pos and free its child node_pos
					printf("\n\t\t\t\tNo key found\n");
					if(starting_node_pos->parent) {
						printf("\n\t\t\t\tMOVING TO PARENT\n");
						print_single_node(btree, starting_node_pos->parent->node);
						starting_node_pos = starting_node_pos->parent;	
						destroy_node_pos(starting_node_pos->child);	
						//return kp;
					} else {	
						printf("\n\t\t\t\tNOTHING FOUND\n");
						starting_node_pos->node = NULL;	
						starting_node_pos->parent = NULL;
						starting_node_pos->child = NULL;
						return kp;
					}
				}

*/
/**
*	Function used to get the node containing the given key
*	@param btree The btree to be searched
*	@param key The the key to be searched
*	@return The node and position of the key within the node 
*/

void create_child_node_pos(node_pos *node_pos) {
	printf("\nCREATING ANOTHER CHILD_NODE_POS\n");
	node_pos->child = create_node_pos();
	node_pos->child->parent = node_pos;
	node_pos->child->index = node_pos->index;
}


// returns number of nodes accessed to get to parent, returns -1 if the node has no parent or does not have any parent that has more children to process
int setNearestParent(btree *btree, node_pos **starting_node_pos) {
	int result = -1;
	while((*starting_node_pos)->parent) {
		printf("\n\t\t\tNode has a parent\n");
		*starting_node_pos = (*starting_node_pos)->parent;
		printf("\n\t\tNOW is now\n");
		print_single_node(btree, (*starting_node_pos)->node);
		// does it have more children to check
		if((*starting_node_pos)->child_pos < (*starting_node_pos)->number_of_possible_children) {
			result++;
			break;
		}		
	}
	return result;
}


int get_btree_node_duplicate(btree * btree, node_pos *starting_node_pos, void * key) {

	printf("\n\t\tIn get_btree_node searching for %d\n", btree->value(key));
	unsigned int key_val = btree->value(key);
	bt_node * node = starting_node_pos->node;
	node_pos *tail = starting_node_pos;
	node_pos kp;
	unsigned int number_of_records = 0;
	 
	// continue with usual search	 
	for (starting_node_pos->index = 0;;) {	

		printf("\n\t\t\t\tWe have this to search\n");
		print_subtree(btree, starting_node_pos->node);
		printf("\n\t\t\t\tFrom Here\n");
		print_single_node(btree, starting_node_pos->node);

		// find node using singular version of this function
		kp = get_btree_node(btree, starting_node_pos, key);

		if(kp.node) {
			printf("\n\t\t\tFound a node\n");	
			starting_node_pos->node = kp.node;
			starting_node_pos->key_val = kp.node->key_vals[kp.index++];
			starting_node_pos->index = kp.index;
			tail = starting_node_pos;
			number_of_records++;
			// is next key_val also the key
			printf("\nnumber of keys is %d, current index is %d\n", kp.node->nr_active, kp.index);	
			while(kp.index < kp.node->nr_active && btree->value(kp.node->key_vals[kp.index]->key) == key_val){
				printf("\n\t\tMore target values at this node\n");
				create_child_node_pos(starting_node_pos);
				starting_node_pos = starting_node_pos->child;
				starting_node_pos->node = kp.node;
				starting_node_pos->key_val = kp.node->key_vals[kp.index++];		
				starting_node_pos->index = kp.index;
				tail = starting_node_pos;
				number_of_records++;
			}
			
			// if its a leaf
			if(starting_node_pos->node->leaf) {
				printf("\n\t\t\tnode is a leaf\n");
				if(setNearestParent(btree, &starting_node_pos) == -1) {
					printf("\n\t\tFinished search\n");
					return number_of_records;
				}
				printf("\n\tNode is now\n");					
				print_single_node(btree, starting_node_pos->node);
			}				
			
			if(starting_node_pos->number_of_possible_children == 0)	// if the child pos has not already been set
				starting_node_pos->number_of_possible_children = (((starting_node_pos->node->nr_active + 1) + starting_node_pos->index) % (starting_node_pos->node->nr_active + 1)) + 1;
			printf("\n\t\t\t\tNumber of possible children %d\n", starting_node_pos->number_of_possible_children);
			// this conditional checks if we still havent searched all child nodes of the current node	
			create_child_node_pos(tail);
			tail = tail->child;
			tail->node = starting_node_pos->node->children[starting_node_pos->child_pos++];
			print_single_node(btree, tail->node);
			starting_node_pos = tail;	
			starting_node_pos->index = 0; // reset the index back to 0
		} else {	// node key was found, we could however have finished one subtree or one route and have other routes to find
			if(setNearestParent(btree, &starting_node_pos) == -1)
				return number_of_records;
		}
	}
}



node_pos get_btree_node(btree * btree, node_pos *starting_node_pos, void * key) {

	node_pos kp;	
	unsigned int key_val = btree->value(key);
	bt_node * node = starting_node_pos->node;
	unsigned int i = 0;
	
	printf("\n\t\tIn get_btree_node searching for %d\n", btree->value(key));
	
	
	for (i = starting_node_pos->index;;i = 0) {	

	    // Fix the index of the key greater than or equal
	    // to the key that we would like to search

		print_subtree(btree, node);

	    while (i < node->nr_active && key_val > btree->value(node->key_vals[i]->key) )
		    i++;
	
	    // If we find such key return the key-value pair		    
	    if(i < node->nr_active && key_val == btree->value(node->key_vals[i]->key)) {
		    //starting_node_pos->node = node;
		    //starting_node_pos->index = i;			

		    kp.node = node;
		    kp.index = i;	
		    return kp;
	    }
	
	    // If the node is leaf and if we did not find the key 
	    // return NULL
	    if(node->leaf) {
		kp.node = NULL;
		starting_node_pos->node = NULL;
		return kp;
	    }
	
	    // To got a child node 
	    node = node->children[i];
	}

	starting_node_pos->node = NULL;
      	return kp;
}



/**
*       Function used to search a node in a B-Tree
*       @param btree The B-tree to be searched
*       @param key Key of the node to be search
*       @return The key-value pair
*/

bt_key_val * btree_search(btree * btree,void * key) {

	bt_key_val * key_val = NULL;

	node_pos *starting_node_pos = malloc(sizeof(node_pos));
	starting_node_pos->node = btree->root;
	starting_node_pos->index = 0;

	node_pos kp = get_btree_node(btree, starting_node_pos, key);
	if(kp.node != NULL) {
		if(kp.node) 
			key_val = kp.node->key_vals[kp.index];
	}

	destroy_node_pos(starting_node_pos);
	return key_val; 
}


int btree_search_subtree(btree *btree, node_pos *starting_node_pos, void *key) {
	return get_btree_node_duplicate(btree, starting_node_pos, key);
}



/**
*       Used to copy key value from source to destination
*       @param src The source key value
*       @param dst The dest key value
*       @return none
*/
static void copy_key_val(btree * btree, bt_key_val * src, bt_key_val * dst) {
        unsigned int keysize;
        unsigned int datasize;

	printf("\nMay be a future error\n");	
	keysize    = btree->key_size(src->key);
	dst->key        = (void *)mem_alloc(keysize);
        bcopy(src->key,dst->key,keysize);
        if(src->val) {
                datasize   = btree->data_size(src->val);
                dst->val       = (void *)mem_alloc(datasize);
                bcopy(src->val,dst->val,datasize);
        }                
        
}

/**
*	Get the max key in the btree
*	@param btree The btree
*	@return The max key 
*/
void * btree_get_max_key(btree * btree) {
	node_pos node_pos;
	node_pos = get_max_key_pos(btree,btree->root);
	return node_pos.node->key_vals[node_pos.index]->key;
}

/**
*	Get the min key in the btree
*	@param btree The btree
*	@return The max key 
*/
void * btree_get_min_key(btree * btree) {
	node_pos node_pos;
	node_pos = get_min_key_pos(btree,btree->root);
	return node_pos.node->key_vals[node_pos.index]->key;
}



//#ifdef DEBUG

/**
*	Used to print the keys of the bt_node
*	@param node The node whose keys are to be printed
*	@return none
*/

void print_single_node(btree *btree, bt_node * node) {
	
	int i = 0;
	
	print("{");	
	while(i < node->nr_active) {
		print("\t%d, %d\t", *(int *) node->key_vals[i]->key, *(int *) node->key_vals[i]->val);
		i++;
	}
	print("}");
}

/**
*       Function used to print the B-tree
*       @param root Root of the B-Tree
*       @param print_key Function used to print the key value
*       @return none
*/

void print_subtree(btree *btree,bt_node * node) {
	
	int i = 0;
	unsigned int current_level;

	bt_node * head, * tail;
	bt_node * child;

	current_level = node->level;
	head = node;
	tail = node; // used to form a linked list of the nodes to be printed out in order

	while(true) {
		if(head == NULL) {
			break;
		}

		// when all nodes at the current level have been processed
		if (head->level < current_level) {
			current_level = head->level;
			print("\n");
		}

		// print out the nodes key-value pairs
		print_single_node(btree,head);

		// if this is a non-leaf node		
		if(head->leaf == false) {
			// for each child node of this node (which is always 1 more then active keys)	
			for(i = 0 ; i < head->nr_active + 1; i++) {
				child = head->children[i];
				tail->next = child; // add this child to the linked list of nodes
				tail = child; 
				child->next = NULL;
			}
		}
	
		head = head->next;	// move to the next node in the list to be printed
	}
	print("\n");
}



//#endif
