#[macro_export]
macro_rules! field_traits {
    (
        $(
            $field_name:ident => {
                $(
                    $trait_prefix:ident :: $method_name:ident $( ( $($param:ident : $param_type:ty),* ) )? $( -> $return_type:ty )?
                ),* $(,)?
            }
        ),* $(,)?
    ) => {
        $(
            paste::paste! {
                pub trait [<Cell $field_name Accessor>]: crate::io::traits::BascetCell {
                    $(
                        $crate::__gen_if_accessor! {
                            $trait_prefix, [<get_ $field_name:snake>] $( ( $($param : $param_type),* ) )? $( -> $return_type )?
                        }
                    )*
                }

                pub trait [<Cell $field_name Builder>]: crate::io::traits::BascetCellBuilder + Sized {
                    $(
                        $crate::__gen_if_builder! {
                            $trait_prefix, [<add_ $field_name:snake>] $( ( $($param : $param_type),* ) )? $( -> $return_type )?
                        }
                    )*
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! __gen_if_accessor {
    (Accessor, $method_name:ident ( $($param:ident : $param_type:ty),* ) -> $return_type:ty) => {
        fn $method_name(&self, $($param: $param_type),*) -> $return_type {
            unimplemented!("{} not implemented", stringify!($method_name))
        }
    };
    (Accessor, $method_name:ident ( $($param:ident : $param_type:ty),* )) => {
        fn $method_name(&self, $($param: $param_type),*) {
            unimplemented!("{} not implemented", stringify!($method_name))
        }
    };
    (Accessor, $method_name:ident -> $return_type:ty) => {
        fn $method_name(&self) -> $return_type {
            unimplemented!("{} not implemented", stringify!($method_name))
        }
    };
    (Accessor, $method_name:ident) => {
        fn $method_name(&self) {
            unimplemented!("{} not implemented", stringify!($method_name))
        }
    };
    (Builder, $method_name:ident $($rest:tt)*) => {};
}

#[macro_export]
macro_rules! __gen_if_builder {
    (Builder, $method_name:ident ( $($param:ident : $param_type:ty),* ) -> $return_type:ty) => {
        fn $method_name(self, $($param: $param_type),*) -> $return_type {
            unimplemented!("{} not implemented", stringify!($method_name))
        }
    };
    (Builder, $method_name:ident ( $($param:ident : $param_type:ty),* )) => {
        fn $method_name(self, $($param: $param_type),*) -> Self {
            unimplemented!("{} not implemented", stringify!($method_name))
        }
    };
    (Builder, $method_name:ident -> $return_type:ty) => {
        fn $method_name(self) -> $return_type {
            unimplemented!("{} not implemented", stringify!($method_name))
        }
    };
    (Builder, $method_name:ident) => {
        fn $method_name(self) -> Self {
            unimplemented!("{} not implemented", stringify!($method_name))
        }
    };
    (Accessor, $method_name:ident $($rest:tt)*) => {};
}
