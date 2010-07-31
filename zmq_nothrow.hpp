/*
 Copyright (c) 2007-2010 iMatix Corporation

 This is free software; you can redistribute it and/or modify it under
 the terms of the Lesser GNU General Public License as published by
 the Free Software Foundation; either version 3 of the License, or
 (at your option) any later version.
 
 This is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 Lesser GNU General Public License for more details.
 
 You should have received a copy of the Lesser GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 
 (This module is a derivative work of zmq.hpp and is released under the
 original copyright. MJW)
 */

/*
 * This module is a "throw nothing" version of zmq.hpp.
 *
 * Each object type must be initialized with a call to the appropriate
 * init() member function after creation and before use, infra vide.
 * 
 *
 * Other changes:
 *
 * operator void*() has been added to context_t to allow interoperation
 * with standard zmq_ calls
 *
 *
 * Motivation: 
 *
 * zmq:: objects are very useful, however in legacy systems with mixed 
 * C/C++ code bases, try-catch is often not fully implemented throughout 
 * the system.  This module provides the advantages of class-based access
 * to ØMQ without creating the need to pepper try-catch statements 
 * throughout the codebase.
 * 
 */

#ifndef __ZMQ_HPP__NOTHROW_INCLUDED__
#define __ZMQ_HPP__NOTHROW_INCLUDED__

#include "zmq.h"

#include <assert.h>

namespace zmq {
	namespace nothrow {
		
		typedef zmq_free_fn free_fn;
		typedef zmq_pollitem_t pollitem_t;
		
		inline int poll(zmq_pollitem_t *items_, int nitems_, long timeout_);
		inline int device(int device_, void * insocket_, void* outsocket_);
		
		inline int poll(zmq_pollitem_t *items_, int nitems_, long timeout_ = -1)
		{
			return zmq_poll(items_, nitems_, timeout_);
		}
		
		inline int device (int device_, void * insocket_, void* outsocket_)
		{
			return zmq_device(device_, insocket_, outsocket_);
		}
		
		class message_t : private zmq_msg_t
			{
				friend class socket_t;
				
			public:
				
				inline message_t()
				{
					// initialize to known value
					content = this;
				}
				
				inline int init()
				{
					assert(content == this);
					
					return zmq_msg_init(this);
				}
				
				inline int init(size_t size_) 
				{
					assert(content == this);

					return zmq_msg_init_size(this, size_);
				}
				
				inline int init(void *data_, size_t size_, free_fn *ffn_, void *hint_ = NULL) 
				{
					assert(content == this);

					return zmq_msg_init_data(this, data_, size_, ffn_, hint_);
				}
				
				inline ~message_t ()
				{
					// NB does not assert
					if (content != this)
						zmq_msg_close(this);
				}
				
				inline int rebuild()
				{
					assert(content != this);
					
					int rc = zmq_msg_close(this);
					if (rc)
						return rc;
					
					return zmq_msg_init(this);
				}
				
				inline int rebuild(size_t size_)
				{
					assert(content != this);
					
					int rc = zmq_msg_close(this);
					if (rc)
						return rc;
					
					return zmq_msg_init_size (this, size_);
				}
				
				inline int rebuild(void *data_, size_t size_, free_fn *ffn_, void *hint_ = NULL)
				{
					assert(content != this);
					
					int rc = zmq_msg_close(this);
					if (rc)
						return rc;
					
					return zmq_msg_init_data(this, data_, size_, ffn_, hint_);
				}
				
				inline int move(message_t *msg_)
				{
					assert(content != this);
					assert(msg_ && msg_->content != msg_);

					return zmq_msg_move(this, msg_);
				}
				
				inline int copy(message_t *msg_)
				{
					assert(content != this);
					assert(msg_ && msg_->content != msg_);
					return zmq_msg_copy(this, msg_);
				}
				
				inline void *data()
				{
					assert(content != this);
					return zmq_msg_data(this);
				}
				
				inline size_t size() const
				{
					assert(content != this);
					return zmq_msg_size(const_cast<message_t*>(this));
				}
				
			private:
				
				//  Disable implicit message copying, so that users won't use shared
				//  messages (less efficient) without being aware of the fact.
				message_t(const message_t&);
				void operator=(const message_t&);
			};
		
		class context_t
			{
				friend class socket_t;
				
			public:
				
				inline context_t() : ptr(0)
				{
				}
				
				int init(int io_threads)
				{
					assert(ptr == NULL);

					ptr = zmq_init(io_threads);

					return ptr ? 0 : -1;
				}
				
				inline ~context_t()
				{
					if (ptr)
						assert(zmq_term(ptr) == 0);
				}
				
				inline operator void*()
				{
					assert(ptr != NULL);

					return ptr;
				}
				
			private:
				
				void *ptr;
				
				context_t(const context_t&);
				void operator=(const context_t&);
			};
		
		class socket_t
			{
				
			public:
				
				inline socket_t() : ptr(0)
				{
				}
				
				int init(context_t &context_, int type_)
				{
					assert(ptr == NULL);
					
					ptr = zmq_socket(context_.ptr, type_);
					
					return ptr ? 0 : -1;
				}
				
				inline ~socket_t()
				{
					if (ptr)
						assert(zmq_close(ptr) == 0);
				}
				
				inline operator void*()
				{
					assert(ptr != NULL);

					return ptr;
				}
				
				inline int setsockopt(int option_, const void *optval_,
									  size_t optvallen_) __attribute__((warn_unused_result))
				{
					assert(ptr != NULL);
					
					return zmq_setsockopt(ptr, option_, optval_, optvallen_);
				}
				
				inline int getsockopt(int option_, void *optval_,
									  size_t *optvallen_)
				{
					assert(ptr != NULL);
					
					return zmq_getsockopt(ptr, option_, optval_, optvallen_);
				}
				
				inline int bind(const char *addr_)
				{
					assert(ptr != NULL);
					
					return zmq_bind(ptr, addr_);
				}
				
				inline int connect(const char *addr_)
				{
					assert(ptr != NULL);
					
					return zmq_connect (ptr, addr_);
				}
				
				inline int send(message_t &msg_, int flags_ = 0)
				{
					assert(ptr != NULL);
					
					return zmq_send(ptr, &msg_, flags_);
				}
				
				inline int recv(message_t *msg_, int flags_ = 0)
				{
					assert(ptr != NULL);
					
					return zmq_recv(ptr, msg_, flags_);
				}
				
			private:
				
				void *ptr;
				
				socket_t(const socket_t&);
				void operator=(const socket_t&);
			};

	}
}

#endif//__ZMQ_HPP__NOTHROW_INCLUDED__
