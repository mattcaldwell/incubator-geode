package com.gemstone.gemfire.internal.offheap.annotations;

import static com.gemstone.gemfire.internal.offheap.annotations.OffHeapIdentifier.*;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used to mark a reference count increment to an off-heap value: 
 * <ul>
 * <li>When used on a method declaration it indicates that the method called retain on the return value if it is an off-heap reference.</li>
 * <li>When used on a constructor declaration this annotation indicates that field members may be off-heap references and retain will be invoked on the field methods.  This also indicates that the class will have a release method.</li>
 * <li>When used on a parameter declaration it indicates that the method will call retain on the parameter if it is an off-heap reference.</li> 
 * <li>When used on a local variable it indicates that the variable will reference an off-heap value that has been retained.  Typically, the method will also be responsible for releasing the value (unless it is the return value).</li>
 * <li>This annotation is also used to mark fields (instance variables) that will have a retain count during the lifetime of the containing object.  Typically, these fields will have their reference counts decremented in release method.</li>
 * </ul>
 * 
 * One or more OffHeapIdentifiers may be supplied if the developer wishes to link this annotation with other
 * off-heap annotations.
 * 
 * @author rholmes
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.PARAMETER,
              ElementType.METHOD,
              ElementType.CONSTRUCTOR,
              ElementType.FIELD,
              ElementType.LOCAL_VARIABLE})
@Documented
public @interface Retained {
  OffHeapIdentifier[] value() default DEFAULT;
}
